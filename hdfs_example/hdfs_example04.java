import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;

public class hdfs_example04 {

    /**
     *      @param args
     */
    public static void main(String[] args) {

        if (args.length < 1) {
            System.out.print("Need at least one arguments\n");
            System.out.flush();
            return;
        }
        System.out.print("Path: "+args[0]+"\n");
        if (args.length > 1)
            System.out.print("Option: "+args[1]+"\n");
        System.out.flush();

        try {
            Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);
            DistributedFileSystem hdfs = (DistributedFileSystem)fs;

            // Delete HDFS file(s)
            Path targetPath = new Path(args[0]);
            String recursive = new String("-r");
            if (args.length > 1 && args[1].equals(recursive)) {
                boolean isDeleted = hdfs.delete(targetPath, true);
            } else {
                boolean isDeleted = hdfs.delete(targetPath, false);
            }
        } catch (Exception e) {
            // handle exception
            e.printStackTrace();
        }
    }
}
