const mongoose = require("mongoose");

const telemetrySchema = new mongoose.Schema({
	ghostId: { type: String, required: true },
	mediaId: { type: String, required: true },
	action: {
		type: String,
		enum: ["impression", "play", "pause", "complete"],
		required: true,
	},
	genre: { type: String, required: true }, // Crucial for training the AI later
	timestamp: { type: Date, default: Date.now },
});

module.exports = mongoose.model("Telemetry", telemetrySchema);
