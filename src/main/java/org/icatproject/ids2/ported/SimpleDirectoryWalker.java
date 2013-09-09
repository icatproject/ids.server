package org.icatproject.ids2.ported;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.io.DirectoryWalker;

public class SimpleDirectoryWalker extends DirectoryWalker<File> {
	public List<File> walk(File startDirectory) throws IOException {
		List<File> results = new ArrayList<File>();
		walk(startDirectory, results);
		return results;
	}

	@Override
	protected boolean handleDirectory(File directory, int depth, Collection<File> results) {
		results.add(directory);
		return true;
	}

	@Override
	protected void handleFile(File file, int depth, Collection<File> results) {
		results.add(file);
	}
};