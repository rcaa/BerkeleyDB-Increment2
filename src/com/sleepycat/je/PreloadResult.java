package com.sleepycat.je;

public class PreloadResult {
	private PreloadStatus status=PreloadStatus.SUCCESS;

	public PreloadStatus getStatus() {
		return status;
	}

	public void setStatus(PreloadStatus status) {
		this.status = status;
	}
}
