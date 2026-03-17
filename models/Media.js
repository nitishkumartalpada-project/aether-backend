const mongoose = require("mongoose");

const mediaSchema = new mongoose.Schema({
	title: { type: String, required: true },
	description: { type: String, required: true },
	genre: { type: String, required: true },
	type: { type: String, enum: ["video", "short"], required: true },
	masterUrl: { type: String, required: true },
	thumbnailUrl: { type: String, required: true },
	nodeName: { type: String, required: true },
	uploadTimestamp: { type: Date, default: Date.now },

	// --- THE NEW SOCIAL ENGINE ---
	views: { type: Number, default: 0 },
	likes: [{ type: String }], // Array of Ghost IDs
	comments: [
		{
			username: { type: String, required: true },
			text: { type: String, required: true },
			timestamp: { type: Date, default: Date.now },
		},
	],
});

module.exports = mongoose.model("Media", mediaSchema);
