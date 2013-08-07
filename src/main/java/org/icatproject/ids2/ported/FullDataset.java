package org.icatproject.ids2.ported;

import java.io.File;

public class FullDataset {
	private final File dir;
	private final String location;
	private File zipfile;

	public FullDataset(File dir, File zipfile, String location) {
		this.dir = dir;
		this.zipfile = zipfile;
		this.location = location;
	}

	public File getDir() {
		return dir;
	}

	public String getLocation() {
		return location;
	}

	public File getZipfile() {
		return zipfile;
	}

}