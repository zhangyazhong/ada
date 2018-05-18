package daslab.bean;

/**
 * @author zyz
 * @version 2018-05-10
 */
public class Breakpoint {
    private String fileName;
    private int lineNumber;

    public Breakpoint(String fileName, int lineNumber) {
        this.fileName = fileName;
        this.lineNumber = lineNumber;
    }

    public void update(String fileName, int lineNumber) {
        this.fileName = fileName;
        this.lineNumber = lineNumber;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public int getLineNumber() {
        return lineNumber;
    }

    public void setLineNumber(int lineNumber) {
        this.lineNumber = lineNumber;
    }

    @Override
    public String toString() {
        return String.format("%s at line.%d", fileName, lineNumber);
    }
}
