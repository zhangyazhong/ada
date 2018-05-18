package daslab.source;

import com.google.common.collect.Lists;
import daslab.bean.Batch;
import daslab.bean.Breakpoint;
import daslab.context.ProducerContext;
import daslab.utils.AdaLogger;
import daslab.utils.FileHandler;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author zyz
 * @version 2018-05-09
 */
public class HdfsDataSource extends DataSource {
    private String host;
    private Path path;
    private Configuration conf;
    private FileSystem fileSystem;
    private List<Path> fileList;

    private HdfsDataSource(ProducerContext context) {
        super(context);
        try {
            String source = context.get("data.source.path");

            AdaLogger.info(this, String.format("Source is from %s", source));

            Matcher matcher = Pattern.compile("[^/]/[\\w]").matcher(source);
            if (matcher.find()) {
                host = source.substring(0, matcher.start() + 1);
                path = new Path(StringUtils.substringAfter(source, host));
            }

            AdaLogger.info(this, String.format("Connecting to %s/%s.", host, path));

            conf = new Configuration();
            conf.set("fs.defaultFS", host);
            fileList = Lists.newArrayList();
            fileSystem = FileSystem.get(conf);
            RemoteIterator<LocatedFileStatus> remoteIterator = fileSystem.listFiles(path, false);
            while (remoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = remoteIterator.next();
                if (!fileStatus.isDirectory()) {
                    fileList.add(fileStatus.getPath());
                }
            }

            AdaLogger.info(this, String.format("Connected to %s/%s.", host, path));
            AdaLogger.info(this, String.format("Source file count: %d.", fileList.size()));

            fileList.sort(Comparator.comparing(Path::getName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static HdfsDataSource connect(ProducerContext context) {
        return new HdfsDataSource(context);
    }

    @Override
    public Batch next() {
        if (fileList.size() < 1) {
            AdaLogger.warn(this, "No candidate data files found in source path.");
            return null;
        }
        Breakpoint breakpoint = archived();
        int index = 0;
        if (breakpoint != null) {
            for (int i = 0; i < fileList.size(); i++) {
                if (fileList.get(i).getName().equals(breakpoint.getFileName())) {
                    index = i;
                }
            }
        } else {
            breakpoint = new Breakpoint(fileList.get(index).getName(), -1);
        }

//        AdaLogger.info(this, String.format("Read from %s at line.%d.",
//                breakpoint.getFileName(), breakpoint.getLineNumber()));
        AdaLogger.info(this, "Read breakpoint from " + breakpoint.toString());

        switch (getContext().get("data.plan.name")) {
            case "single":
                index = breakpoint.getLineNumber() < 0 ? index - 1 : index;
                if (index == fileList.size()) {
                    AdaLogger.info(this, "Data source has been used up.");
                    return null;
                }
                for (int k = 1; k <= Integer.parseInt(getContext().get("data.plan.count")) && index + k < fileList.size(); k++) {
                    try {
                        InputStream inputStream = fileSystem.open(fileList.get(index + k));
                        File outputFile = new File(getContext().get("data.batch.tmp.folder") + (k - 1) + ".dat");
                        if (!outputFile.getParentFile().exists()) {
                            outputFile.getParentFile().mkdirs();
                        }
                        OutputStream outputStream =
                                new FileOutputStream(getContext().get("data.batch.tmp.folder") + (k - 1) + ".dat");
                        IOUtils.copyBytes(inputStream, outputStream, 1024, true);

                        AdaLogger.info(this, String.format("Copy %s to local is finished.",
                                fileList.get(index + k).getName()));

                        breakpoint.update(fileList.get(index + k).getName(), 0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                File[] files = new File[Math.min(Integer.parseInt(getContext().get("data.plan.count")), fileList.size() - index)];
                for (int k = 1; k <= Integer.parseInt(getContext().get("data.plan.count")) && index + k < fileList.size(); k++) {
                    files[k - 1] = new File(getContext().get("data.batch.tmp.folder") + (k - 1) + ".dat");
                }
                File file = FileHandler.merge(files, getContext().get("data.batch.location"));

                AdaLogger.info(this, "Update breakpoint to " + breakpoint.toString());

                setBreakpoint(breakpoint);
                return new Batch(file);
        }

        return null;
    }

}
