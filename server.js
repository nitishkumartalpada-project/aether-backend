require("dotenv").config();
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const { exec } = require("child_process");
const util = require("util");
const execPromise = util.promisify(exec);
const axios = require("axios");
const simpleGit = require("simple-git");
const mongoose = require("mongoose");
const Redis = require("ioredis");

const Media = require("./models/Media");

const app = express();
app.set("trust proxy", 1);
app.use(cors());
app.use(express.json());

// --- MONGODB CONNECTION ---
mongoose
	.connect(process.env.MONGODB_URI)
	.then(() => console.log("📦 Connected to MongoDB Aether Brain"))
	.catch((err) => console.error("❌ MongoDB Connection Error:", err));

// --- REDIS STREAM SETUP ---
const redis = new Redis(process.env.REDIS_URL);

redis.on("connect", () => {
	console.log("⚡ Connected to Aether Redis Stream Edge");
});
redis.on("error", (err) => {
	console.error("❌ Redis Edge Connection Error:", err);
});

// --- INGESTION DIRECTORIES ---
const tempDir = path.join(__dirname, "temp_raw");
const processedDir = path.join(__dirname, "processed");

if (!fs.existsSync(tempDir)) fs.mkdirSync(tempDir);
if (!fs.existsSync(processedDir)) fs.mkdirSync(processedDir);

const storage = multer.diskStorage({
	destination: (req, file, cb) => cb(null, tempDir),
	filename: (req, file, cb) => {
		const cleanName =
			Date.now() + "-" + file.originalname.replace(/[^a-zA-Z0-9.]/g, "_");
		cb(null, cleanName);
	},
});
const upload = multer({ storage });

const multiUpload = upload.fields([
	{ name: "mediaFile", maxCount: 1 },
	{ name: "thumbnailFile", maxCount: 1 },
]);

// --- THE GLOBAL PROCESSING QUEUE ---
// This guarantees FFmpeg never runs twice at the same time, saving the 512MB RAM
const processingQueue = [];
let isProcessing = false;

app.post("/api/ingest", multiUpload, (req, res) => {
	console.log("\n📡 --- NEW PAYLOAD INBOUND ---");
	const rawVideoPath = req.files["mediaFile"][0].path;
	const rawThumbPath = req.files["thumbnailFile"][0].path;
	const uniqueNodeName = `aether-${req.body.type}-${Date.now()}`;
	const outputFolder = path.join(processedDir, uniqueNodeName);

	// 1. Immediately respond to the client so the browser doesn't timeout
	res.json({
		success: true,
		message: "Upload secured. Placed in Aether Cloud Queue for processing.",
	});

	// 2. Add to the Global Queue
	processingQueue.push({
		videoPath: rawVideoPath,
		thumbPath: rawThumbPath,
		outputFolder,
		nodeName: uniqueNodeName,
		metadata: req.body,
	});

	console.log(
		`📥 Added ${uniqueNodeName} to queue. Position: ${processingQueue.length}`,
	);

	// 3. Trigger the worker if it's currently sleeping
	if (!isProcessing) {
		processNextInQueue();
	}
});

// --- THE QUEUE WORKER ---
async function processNextInQueue() {
	if (processingQueue.length === 0) {
		isProcessing = false;
		console.log("🛌 Aether Engine Queue Empty. Standing by.");
		return;
	}

	isProcessing = true;
	const job = processingQueue.shift();

	try {
		await executeSequentialTranscode(
			job.videoPath,
			job.thumbPath,
			job.outputFolder,
			job.nodeName,
			job.metadata,
		);
	} catch (error) {
		console.error(`❌ Critical Failure on Job ${job.nodeName}:`, error);
	}

	// Recursively process the next video in line!
	processNextInQueue();
}

// --- SEQUENTIAL TRANSCODER (STRICT RAM LIMITER) ---
async function executeSequentialTranscode(
	videoPath,
	thumbPath,
	outputFolder,
	nodeName,
	metadata,
) {
	console.log(`\n⚙️  Aether Transcoder Initiated for: ${nodeName}`);
	fs.mkdirSync(outputFolder, { recursive: true });

	const thumbExt = path.extname(thumbPath);
	const finalThumbPath = path.join(outputFolder, `thumbnail${thumbExt}`);
	fs.renameSync(thumbPath, finalThumbPath);

	try {
		const { stdout } = await execPromise(
			`ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "${videoPath}"`,
		);
		const originalHeight = parseInt(stdout.trim()) || 1080;
		console.log(
			`📏 Source Height: ${originalHeight}p. Engaging Sequential Safe-Mode Encoding...`,
		);

		// Multi-tier, but processed strictly ONE AT A TIME
		const tiers = [
			{ height: 1080, bit: "3000k", res: "1920x1080", name: "v0" },
			{ height: 720, bit: "1500k", res: "1280x720", name: "v1" },
			{ height: 480, bit: "1000k", res: "854x480", name: "v2" },
		];

		let masterPlaylistContent = "#EXTM3U\n#EXT-X-VERSION:3\n";
		let streamIndex = 0;

		for (const tier of tiers) {
			if (originalHeight >= tier.height - 50 || streamIndex === 0) {
				console.log(`⏳ Encoding ${tier.height}p stream...`);
				const tierFolder = path.join(outputFolder, tier.name);
				fs.mkdirSync(tierFolder);

				// STRICT MEMORY LIMIT COMMAND: 1 Thread, Ultrafast, Buffer Limited
				const ffmpegCmd =
					`ffmpeg -y -threads 1 -i "${videoPath}" ` +
					`-s ${tier.res} -c:v libx264 -preset ultrafast -tune zerolatency ` +
					`-b:v ${tier.bit} -maxrate ${tier.bit} -bufsize ${tier.bit} -max_muxing_queue_size 1024 ` +
					`-f hls -hls_time 6 -hls_list_size 0 -hls_segment_filename "${tierFolder}/fileSequence%d.ts" "${tierFolder}/prog_index.m3u8"`;

				// AWAIT blocks the loop, guaranteeing RAM drops back down between resolutions
				await execPromise(ffmpegCmd);

				const bandwidth = parseInt(tier.bit.replace("k", "000")) * 1.2;
				masterPlaylistContent += `#EXT-X-STREAM-INF:BANDWIDTH=${bandwidth},RESOLUTION=${tier.res}\n${tier.name}/prog_index.m3u8\n`;

				streamIndex++;
			}
		}

		fs.writeFileSync(
			path.join(outputFolder, "master.m3u8"),
			masterPlaylistContent,
		);
		console.log(
			`✅ All streams encoded sequentially. Master playlist generated.`,
		);

		fs.unlinkSync(videoPath);
		await deployToGitHub(outputFolder, nodeName, metadata, thumbExt);
	} catch (error) {
		throw error;
	}
}

async function deployToGitHub(folderPath, repoName, metadata, thumbExt) {
	const GITHUB_USERNAME = process.env.GITHUB_USERNAME;
	const GITHUB_TOKEN = process.env.GITHUB_TOKEN;

	try {
		console.log(`\n☁️  Initializing Cloud Provisioning for: ${repoName}`);
		await axios.post(
			"https://api.github.com/user/repos",
			{ name: repoName, private: false, auto_init: true },
			{ headers: { Authorization: `token ${GITHUB_TOKEN}` } },
		);

		const git = simpleGit(folderPath);
		await git.init();

		// --- ADD THESE TWO EXACT LINES ---
		await git.addConfig("user.name", "Aether Pipeline");
		await git.addConfig("user.email", "aether@pipeline.local");
		// ---------------------------------

		await git.add("./*");
		await git.commit("Aether Core Automated Ingestion");
		await git.branch(["-M", "main"]);

		const remoteUrl = `https://${GITHUB_USERNAME}:${GITHUB_TOKEN}@github.com/${GITHUB_USERNAME}/${repoName}.git`;
		await git.addRemote("origin", remoteUrl);
		await git.push(["-u", "-f", "origin", "main"]);

		console.log(`⚙️  Activating Fastly CDN routing (GitHub Pages)...`);
		await axios.post(
			`https://api.github.com/repos/${GITHUB_USERNAME}/${repoName}/pages`,
			{ source: { branch: "main", path: "/" } },
			{
				headers: {
					Authorization: `token ${GITHUB_TOKEN}`,
					Accept: "application/vnd.github+json",
				},
			},
		);

		const masterUrl = `https://${GITHUB_USERNAME}.github.io/${repoName}/master.m3u8`;
		const thumbUrl = `https://${GITHUB_USERNAME}.github.io/${repoName}/thumbnail${thumbExt}`;

		const newMedia = new Media({
			title: metadata.title,
			description: metadata.description,
			genre: metadata.genre,
			type: metadata.type,
			masterUrl: masterUrl,
			thumbnailUrl: thumbUrl,
			nodeName: repoName,
		});

		await newMedia.save();
		console.log(`🎉 PIPELINE COMPLETE: Database Synced!`);
	} catch (error) {
		console.error("❌ Cloud Deployment Failed:", error.message);
	}
}

// --- UPGRADED GLOBAL FEED (Filters, Sorting, AI) ---
app.get("/api/feed", async (req, res) => {
	try {
		const page = parseInt(req.query.page) || 1;
		const limit = 6;
		const search = req.query.search || "";
		const ghostId = req.query.ghostId;
		const filterGenre = req.query.genre || "All";
		const sortBy = req.query.sort || "recommended";

		let topGenres = [];
		if (ghostId && sortBy === "recommended") {
			topGenres = await redis.zrevrange(
				`aether:profile:${ghostId}`,
				0,
				2,
			);
		}

		const query = {
			type: "video",
			title: { $regex: search, $options: "i" },
		};
		if (filterGenre !== "All") query.genre = filterGenre;

		let sortPipeline = { views: -1 };
		if (sortBy === "views") sortPipeline = { views: -1 };
		if (sortBy === "newest") sortPipeline = { _id: -1 };

		const feedData = await Media.aggregate([
			{ $match: query },
			{
				$addFields: {
					likesCount: { $size: { $ifNull: ["$likes", []] } },
					aiScore: {
						$cond: {
							if: { $in: ["$genre", topGenres] },
							then: 1000,
							else: 0,
						},
					},
				},
			},
			{
				$sort:
					sortBy === "recommended"
						? { aiScore: -1, views: -1 }
						: sortPipeline,
			},
			{ $skip: (page - 1) * limit },
			{ $limit: limit },
		]);

		res.json(feedData);
	} catch (error) {
		console.error("❌ Database Fetch Error:", error);
		res.status(500).json({
			error: "Failed to retrieve feed from Aether Core.",
		});
	}
});

// --- UPGRADED SHORTS FEED (AI Recommendation Added) ---
app.get("/api/shorts", async (req, res) => {
	try {
		console.log("📱 Client requesting AI Shorts feed...");
		const ghostId = req.query.ghostId;

		let topGenres = [];
		if (ghostId) {
			topGenres = await redis.zrevrange(
				`aether:profile:${ghostId}`,
				0,
				2,
			);
		}

		const shortsData = await Media.aggregate([
			{ $match: { type: "short" } },
			{
				$addFields: {
					aiScore: {
						$cond: {
							if: { $in: ["$genre", topGenres] },
							then: 1000,
							else: 0,
						},
					},
				},
			},
			{ $sort: { aiScore: -1, _id: -1 } },
			{ $limit: 20 },
		]);

		res.json(shortsData);
	} catch (error) {
		console.error("❌ Database Fetch Error:", error);
		res.status(500).json({
			error: "Failed to retrieve shorts from Aether Core.",
		});
	}
});

// --- SOCIAL INTERACTION ROUTES ---
app.post("/api/media/:id/view", async (req, res) => {
	try {
		await Media.findByIdAndUpdate(req.params.id, { $inc: { views: 1 } });
		res.json({ success: true });
	} catch (error) {
		res.status(500).json({ error: "Failed to update view count." });
	}
});

app.post("/api/media/:id/like", async (req, res) => {
	try {
		const { ghostId } = req.body;
		const media = await Media.findById(req.params.id);

		if (!media) return res.status(404).json({ error: "Media not found." });

		const hasLiked = media.likes.includes(ghostId);

		if (hasLiked) {
			media.likes = media.likes.filter((id) => id !== ghostId);
		} else {
			media.likes.push(ghostId);
		}

		await media.save();
		res.json({
			success: true,
			likes: media.likes.length,
			hasLiked: !hasLiked,
		});
	} catch (error) {
		res.status(500).json({ error: "Failed to toggle like." });
	}
});

app.post("/api/media/:id/comment", async (req, res) => {
	try {
		const { username, text } = req.body;
		const media = await Media.findById(req.params.id);

		if (!media) return res.status(404).json({ error: "Media not found." });
		const clientIp =
			req.headers["x-forwarded-for"] || req.socket.remoteAddress;

		const newComment = {
			username,
			text,
			ip: clientIp,
			timestamp: new Date(),
		};

		media.comments.unshift(newComment);
		await media.save();

		res.json({ success: true, comment: newComment });
	} catch (error) {
		res.status(500).json({ error: "Failed to post comment." });
	}
});

app.get("/api/media/:id", async (req, res) => {
	try {
		const media = await Media.findById(req.params.id);
		if (!media) return res.status(404).json({ error: "Media not found." });

		media.views += 1;
		await media.save();

		res.json(media);
	} catch (error) {
		res.status(500).json({ error: "Failed to fetch media." });
	}
});
// --- TELEMETRY ROUTES ---
app.post("/api/telemetry", async (req, res) => {
	try {
		const { ghostId, mediaId, genre, watchTimeSeconds } = req.body;

		if (watchTimeSeconds >= 2) {
			const telemetryEvent = {
				ghostId,
				mediaId,
				genre,
				watchTimeSeconds,
				timestamp: new Date().toISOString(),
			};

			await redis.xadd(
				"aether:telemetry:stream",
				"*",
				"payload",
				JSON.stringify(telemetryEvent),
			);
			await redis.zincrby(
				`aether:profile:${ghostId}`,
				watchTimeSeconds,
				genre,
			);
			console.log(
				`📡 Telemetry Updated: Ghost [${ghostId}] watched ${watchTimeSeconds}s of ${genre}`,
			);
		}
		res.status(204).send();
	} catch (error) {
		res.status(500).send();
	}
});

// --- NLP TEXT INDEXING ---
mongoose.connection.once("open", async () => {
	try {
		await Media.collection.createIndex({
			title: "text",
			description: "text",
		});
		console.log("🧠 MongoDB NLP Text Indexing Active");
	} catch (e) {
		console.log("MongoDB Indexing Note:", e.message);
	}
});

// --- PHASE 12: ADMIN CONTROL ROUTES ---
const rateLimit = require("express-rate-limit");

const adminAuthLimiter = rateLimit({
	windowMs: 15 * 60 * 1000,
	max: 5,
	message: {
		error: "Security breach detected. Mainframe locked for 15 minutes.",
	},
	standardHeaders: true,
	legacyHeaders: false,
});

app.post("/api/admin/auth", adminAuthLimiter, (req, res) => {
	const { password } = req.body;
	if (password === process.env.ADMIN_PASSWORD) {
		res.json({ success: true });
	} else {
		res.status(401).json({ error: "Invalid override code." });
	}
});

app.get("/api/admin/media", async (req, res) => {
	try {
		const allMedia = await Media.find().sort({ uploadTimestamp: -1 });
		res.json(allMedia);
	} catch (error) {
		res.status(500).json({ error: "Admin fetch failed." });
	}
});

app.delete("/api/admin/media/:id", async (req, res) => {
	try {
		const media = await Media.findByIdAndDelete(req.params.id);
		if (!media) return res.status(404).json({ error: "Media not found." });
		res.json({
			success: true,
			message: "Media eradicated from Aether Core.",
		});
	} catch (error) {
		res.status(500).json({ error: "Deletion failed." });
	}
});

app.delete(
	"/api/admin/media/:mediaId/comment/:commentTimestamp",
	async (req, res) => {
		try {
			const { mediaId, commentTimestamp } = req.params;
			const media = await Media.findById(mediaId);

			if (!media)
				return res.status(404).json({ error: "Media not found." });

			media.comments = media.comments.filter(
				(c) => new Date(c.timestamp).toISOString() !== commentTimestamp,
			);
			await media.save();

			res.json({ success: true, message: "Comment purged." });
		} catch (error) {
			res.status(500).json({ error: "Comment deletion failed." });
		}
	},
);

// LIMITER 4: Let Render assign the port automatically!
const PORT = process.env.PORT || 5000;
app.listen(PORT, () =>
	console.log(`🚀 Aether Core Engine active on port ${PORT}`),
);
