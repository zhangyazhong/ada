package dsalab.source;

import com.google.common.collect.ImmutableMap;
import dsalab.bean.Batch;
import dsalab.bean.Breakpoint;
import dsalab.context.ProducerContext;
import dsalab.utils.ConfigHandler;

import java.io.File;
import java.util.Properties;

/**
 * @author zyz
 * @version 2018-05-09
 */
public abstract class DataSource {
    private ProducerContext context;
    private Breakpoint breakpoint;

    public DataSource(ProducerContext context) {
        this.context = context;
    }

    public ProducerContext getContext() {
        return context;
    }


    public void setBreakpoint(Breakpoint breakpoint) {
        this.breakpoint = breakpoint;
    }

    public abstract Batch next();

    public void archive() {
        String location = context.get("breakpoint.location");
        Properties properties = ConfigHandler.create(location, ImmutableMap.of(
                "self.location", location,
                "breakpoint.file_name", breakpoint.getFileName(),
                "breakpoint.line_number", String.valueOf(breakpoint.getLineNumber())
        ));
    }

    public Breakpoint archived() {
        String location = context.get("breakpoint.location");
        File file = new File(location);
        if (file.exists()) {
            Properties properties = ConfigHandler.load(location);
            if (properties.getProperty("breakpoint.file_name") != null
                    && properties.getProperty("breakpoint.line_number") != null) {
                breakpoint = new Breakpoint(properties.getProperty("breakpoint.file_name"),
                        Integer.parseInt(properties.getProperty("breakpoint.line_number")));
                return breakpoint;
            }
        }
        return null;
    }
}
