require("dotenv").config();
const express = require("express");
const cors = require("cors");
const multer = require("multer");
const fs = require("fs");
const path = require("path");
const { exec } = require("child_process");
const axios = require("axios");
const simpleGit = require("simple-git");
const mongoose = require("mongoose");
const Redis = require("ioredis");

const Media = require("./models/Media");

const app = express();
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

// --- PHASE 4 & 5: DYNAMIC UPLOAD & DEPLOY ROUTE ---
app.post("/api/ingest", multiUpload, (req, res) => {
	console.log("\n📡 --- NEW PAYLOAD INBOUND ---");
	const rawVideoPath = req.files["mediaFile"][0].path;
	const rawThumbPath = req.files["thumbnailFile"][0].path;
	const uniqueNodeName = `aether-${req.body.type}-${Date.now()}`;
	const outputFolder = path.join(processedDir, uniqueNodeName);

	res.json({
		success: true,
		message: "Upload secured. Processing & Deploying to Cloud Edge.",
	});

	processMedia(
		rawVideoPath,
		rawThumbPath,
		outputFolder,
		uniqueNodeName,
		req.body,
	);
});

function processMedia(videoPath, thumbPath, outputFolder, nodeName, metadata) {
	console.log(`\n⚙️  Aether Transcoder initiated for: ${nodeName}`);
	fs.mkdirSync(outputFolder, { recursive: true });

	const thumbExt = path.extname(thumbPath);
	const finalThumbPath = path.join(outputFolder, `thumbnail${thumbExt}`);
	fs.renameSync(thumbPath, finalThumbPath);

	exec(
		`ffprobe -v error -select_streams v:0 -show_entries stream=height -of csv=p=0 "${videoPath}"`,
		(err, stdout) => {
			const originalHeight = parseInt(stdout.trim()) || 1080;
			console.log(
				`📏 Detected Source Resolution Height: ${originalHeight}p`,
			);

			let ffmpegCmd = `ffmpeg -i "${videoPath}" `;
			let maps = "";
			let scales = "";
			let streamMap = `-var_stream_map "`;
			let streamIndex = 0;

			const tiers = [
				{ height: 1080, bit: "3000k", res: "1920x1080" },
				{ height: 720, bit: "1500k", res: "1280x720" },
				{ height: 480, bit: "1000k", res: "854x480" },
				{ height: 360, bit: "600k", res: "640x360" },
			];

			tiers.forEach((tier) => {
				if (originalHeight >= tier.height - 50 || streamIndex === 0) {
					maps += `-map 0:v:0 -map 0:a:0 `;
					scales += `-s:v:${streamIndex} ${tier.res} -c:v:${streamIndex} libx264 -b:v:${streamIndex} ${tier.bit} `;
					streamMap += `v:${streamIndex},a:${streamIndex} `;
					fs.mkdirSync(path.join(outputFolder, `v${streamIndex}`));
					streamIndex++;
				}
			});
			streamMap = streamMap.trim() + `" `;

			ffmpegCmd +=
				maps +
				scales +
				streamMap +
				`-master_pl_name master.m3u8 -f hls -hls_time 6 -hls_list_size 0 -hls_segment_filename "v%v/fileSequence%d.ts" "v%v/prog_index.m3u8"`;

			exec(ffmpegCmd, { cwd: outputFolder }, (error) => {
				if (error) {
					console.error(`❌ Transcoder Failed:`, error);
					return;
				}
				console.log(`✅ Transcoder Complete. Cleaning up raw video...`);
				fs.unlinkSync(videoPath);
				deployToGitHub(outputFolder, nodeName, metadata, thumbExt);
			});
		},
	);
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
		console.log(`\n🎉 PIPELINE COMPLETE: Database Synced!`);
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

// --- PHASE 10 & 11: REDIS TELEMETRY & SHADOW PROFILING ---
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
				`📡 Telemetry Profile Updated: Ghost [${ghostId}] watched ${watchTimeSeconds}s of ${genre}`,
			);
		}

		res.status(204).send();
	} catch (error) {
		console.error("Redis Telemetry Drop:", error.message);
		res.status(500).send();
	}
});

// --- NLP TEXT INDEXING (For Description & Title AI) ---
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

const rateLimit = require("express-rate-limit");

// --- SECURE ADMIN AUTHENTICATION ---
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

// --- PHASE 12: ADMIN CONTROL ROUTES ---
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

const PORT = 5000;
app.listen(PORT, () =>
	console.log(`🚀 Aether Core Engine active on port ${PORT}`),
);
