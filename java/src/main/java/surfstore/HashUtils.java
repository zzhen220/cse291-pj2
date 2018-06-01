package surfstore;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HashUtils {

    //compute hash value of an array of byte
    public static String sha256(byte[] data){
        MessageDigest digest = null;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e){
            e.printStackTrace();
            System.exit(2);
        }
        byte[] hash = digest.digest(data);
        String encoded = Base64.getEncoder().encodeToString(hash);
        return encoded;
    }

    //compute the hashlist corresponding to the give file. Save blocks in the given map.
    public static List<String> compute_hashlist(String filePath, Map<String, byte[]> map) throws IOException{
        List<String> res = new ArrayList<>();
        byte[] buffer = new byte[4096];
        byte[] tempBuffer = new byte[4096];
        try (InputStream is = Files.newInputStream(Paths.get(filePath))) {
            int readByte;
            while ((readByte = is.read(buffer)) >= 0) {
                if(readByte < 4096)
                    tempBuffer = new byte[readByte];
                    System.arraycopy(buffer, 0, tempBuffer, 0, readByte);
                    buffer = tempBuffer;
                String encoded = sha256(buffer);
                res.add(encoded);
                map.put(encoded, buffer.clone());
            }
        } catch (IOException e){
            throw e;
        }
        return res;
    }

    public static void main(String[] args) throws Exception{
        System.out.println(HashUtils.sha256(new String("Are you ready?").getBytes(StandardCharsets.UTF_8)));
        Map<String, byte[]> map = new HashMap<>();
        List<String> res = compute_hashlist("/home/zzhen/class/18sp/cse291/p2-p2-jpzz/test/test.txt", map);
        for(String s:res){
            System.out.println(s);
        }
    }
}
