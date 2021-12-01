package org.tensorflow.hadoop.io;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

public class NoEmptyFileOutputCommitter extends FileOutputCommitter{
    public NoEmptyFileOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        super(outputPath, context);
    }

    public NoEmptyFileOutputCommitter(Path outputPath, JobContext context) throws IOException {
        super(outputPath, context);
    }

    @Override
    public void commitTask(TaskAttemptContext context)
            throws IOException {
        Path taskAttemptPath = getTaskAttemptPath(context);
        Path committedTaskPath = getCommittedTaskPath(context);
        FileSystem fs = taskAttemptPath.getFileSystem(context.getConfiguration());
        FileStatus[] listStatus = fs.listStatus(taskAttemptPath);
        if(listStatus.length == 0){
            return;
        }
        long length = 0;
        for(int i =0; i < listStatus.length; i++){
            if (listStatus[i].isDirectory()) {
                length += fs.getContentSummary(listStatus[i].getPath()).getLength();
            } else {
                length += listStatus[i].getLen();
            }
        }
        if (length == 0){
            return;
        }else{
            // deleteEmptyDir(fs, taskAttemptPath);
            commitTask(context, null);
        }
    }

    public static void deleteEmptyDir(FileSystem fs,Path path) throws IOException{

        // if it is empty directory,delete it
        FileStatus[] listStatus = fs.listStatus(path);
        if(listStatus.length == 0){
            fs.delete(path,true);
            return;
        }

        /**
         * If the given path is not an empty folder, all files and folders under the path are obtained
         */
        RemoteIterator<LocatedFileStatus> ls = fs.listLocatedStatus(path);
        while(ls.hasNext()){
            LocatedFileStatus next = ls.next();
            Path currentpath = next.getPath();

            /**
             * Delete an empty file or folder
             */
            if(next.isDirectory()){
                if(fs.listStatus(currentpath).length == 0){
                    fs.delete(currentpath,true);
                }else{
                    deleteEmptyDir(fs,currentpath);
                }
            }else{
                if(next.getLen() == 0){
                    fs.delete(currentpath,true);
                }
            }

//            /**
//             * If it becomes an empty folder, delete it too
//             */
//            if(fs.listStatus(parentPath).length == 0){
//                fs.delete(parentPath,true);
//            }
        }
    }
}

