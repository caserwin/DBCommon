package jdbc.service;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by yidxue on 2018/6/13
 */
public class FileUtil {

    public static void changeFolderPermission(String path, boolean curr) {
        File dirFile = new File(path);
        Set<PosixFilePermission> perms = new HashSet<>();
        perms.add(PosixFilePermission.OWNER_READ);
        perms.add(PosixFilePermission.OWNER_WRITE);
        perms.add(PosixFilePermission.OWNER_EXECUTE);
        perms.add(PosixFilePermission.GROUP_READ);
        perms.add(PosixFilePermission.GROUP_WRITE);
        perms.add(PosixFilePermission.GROUP_EXECUTE);
        perms.add(PosixFilePermission.OTHERS_READ);
        perms.add(PosixFilePermission.OTHERS_WRITE);
        perms.add(PosixFilePermission.OTHERS_EXECUTE);

        try {
            Files.setPosixFilePermissions(Paths.get(dirFile.getAbsolutePath()), perms);
        } catch (IOException e) {
            System.out.println("Operation not permitted !!");
        }

        if (curr) {
            File parentFile = dirFile.getParentFile();
            if (parentFile != null) {
                changeFolderPermission(parentFile.getPath(), true);
            }
        }
    }

    public static void writeByStream(List<String> data, String path) {
        try {
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path)));
            for (String aData : data) {
                bw.append(aData).append("\r\n");
            }
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static ArrayList<String> readByStream(String path) {
        ArrayList<String> resLs = new ArrayList<>();
        try {

            BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
            String line;
            while ((line = br.readLine()) != null) {
                resLs.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return resLs;
    }
}