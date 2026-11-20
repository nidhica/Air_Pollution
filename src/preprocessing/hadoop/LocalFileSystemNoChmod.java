package aeroscope.fs;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsPermission;

/**
 * Local filesystem used for Spark Parquet output when winutils.exe is absent.
 *
 * Hadoop's local filesystem asks winutils or Hadoop's Windows native library
 * to chmod and to test directory access. Neither is installed here. Directory
 * creation, listing, and file status stay on the Java file APIs so Spark can
 * commit part files.
 */
public class LocalFileSystemNoChmod extends RawLocalFileSystem {
    @Override
    public void setPermission(Path path, FsPermission permission) throws IOException {
        String operatingSystem = System.getProperty("os.name", "");
        if (operatingSystem.toLowerCase().startsWith("windows")) {
            return;
        }
        super.setPermission(path, permission);
    }

    @Override
    public FileStatus[] listStatus(Path path) throws IOException {
        File local = pathToFile(path);
        if (!local.exists()) {
            throw new FileNotFoundException("File " + path + " does not exist");
        }
        if (!local.isDirectory()) {
            return new FileStatus[] {statusOf(path, local)};
        }
        String[] names = local.list();
        if (names == null) {
            throw new IOException("Cannot list " + path);
        }
        FileStatus[] results = new FileStatus[names.length];
        for (int index = 0; index < names.length; index++) {
            File child = new File(local, names[index]);
            results[index] = statusOf(new Path(path, names[index]), child);
        }
        return results;
    }

    @Override
    public FileStatus getFileStatus(Path path) throws IOException {
        File local = pathToFile(path);
        if (!local.exists()) {
            throw new FileNotFoundException("File " + path + " does not exist");
        }
        return statusOf(path, local);
    }

    private FileStatus statusOf(Path path, File local) throws IOException {
        long length = local.isDirectory() ? 0L : local.length();
        return new FileStatus(
            length,
            local.isDirectory(),
            1,
            getDefaultBlockSize(path),
            local.lastModified(),
            makeQualified(path)
        );
    }
}
