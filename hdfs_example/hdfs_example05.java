import java.util.Date;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.*;

public class hdfs_example05 {

    /**
     *      @param args
     */
    public static void main(String[] args) {

        System.out.print("Path: "+args[0]+"\n");
        System.out.flush();

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            DistributedFileSystem hdfs = (DistributedFileSystem)fs;
            Path targetPath = new Path(args[0]);

            // Check if a file or directory exists in HDFS
            if (!hdfs.exists(targetPath)) {
			    System.out.print("File/Directory not found!\n");
                System.out.flush();
                return;
            }

            // Check the status of a file or directory in HDFS
            FileStatus fileStatus = hdfs.getFileStatus(targetPath);
            if (fileStatus.isDir())
                System.out.print("Type: Directory\n");
            else
                System.out.print("Type: File\n");
            System.out.print("Owner: "+fileStatus.getOwner()+"\n");
            System.out.print("Group: "+fileStatus.getGroup()+"\n");
            System.out.print("Length: "+fileStatus.getLen()+"\n");
            System.out.print("BlockSize: "+fileStatus.getBlockSize()+"\n");
            System.out.print("Replication: "+fileStatus.getReplication()+"\n");
            System.out.print("Permission: "+fileStatus.getPermission()+"\n");
            Date modifyTime = new Date(fileStatus.getModificationTime());
            System.out.print("Modified: "+modifyTime+"\n");
            System.out.flush();
        } catch (Exception e) {
            // handle exception
             e.printStackTrace();
        }
    }
}