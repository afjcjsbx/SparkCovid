package utils;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;

public class FileUtils implements Serializable {
    private String name;
    private String path;

    public FileUtils(String name, String path) {
        this.name = name;
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public static void renameFile(File toBeRenamed, String new_name)
            throws IOException {
        //need to be in the same path
        File fileWithNewName = new File(toBeRenamed.getParent(), new_name);
        if (fileWithNewName.exists()) {
            throw new IOException("file exists");
        }
        // Rename file (or directory)
        boolean success = toBeRenamed.renameTo(fileWithNewName);
        if (!success) {
            // File was not successfully renamed
        }
    }
}