import org.apache.commons.codec.binary.Hex;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
public class Md5Util {
    public static String md5(byte[] key) {
        return md5(key, 0, key.length);
    }

    public static String md5(byte[] key, int offset, int length) {
        try {
            MessageDigest e = MessageDigest.getInstance("MD5");
            e.update(key, offset, length);
            byte[] digest = e.digest();
            return new String(Hex.encodeHex(digest));
        } catch (NoSuchAlgorithmException var5) {
            throw new RuntimeException("Error computing MD5 hash", var5);
        }
    }

    public static String md5(String str) {
        return md5(str.getBytes());
    }
    public static String md5(String str,int offset, int length) {
        return md5(str.getBytes(),offset,length);
    }
}