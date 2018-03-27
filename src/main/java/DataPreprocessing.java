import java.io.*;
import java.nio.channels.FileChannel;

public class DataPreprocessing {
    public static void main(String[] args) throws IOException{
        String srcPath = "/home/wenning/Desktop/plaintext_articles";
        String destPath = "/home/wenning/Desktop/merge.txt";

        File src = new File(srcPath);
        File dest = new File(destPath);
        if (!dest.exists()) {
            try {
                dest.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        File[] files = src.listFiles();

        FileChannel outChannel = new FileOutputStream(dest).getChannel();

        for (File file : files) {
            FileChannel inChannel = new FileInputStream(file).getChannel();
            inChannel.transferTo(0, inChannel.size(), outChannel);
            inChannel.close();
        }

        outChannel.close();
    }
}
