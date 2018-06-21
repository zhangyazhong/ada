package daslab.inspector;

public class TableColumnSample {
    private TableColumn column;
    private double ratio;

    public TableColumnSample(TableColumn column, double ratio) {
        this.column = column;
        this.ratio = ratio;
    }

    public TableColumn getColumn() {
        return column;
    }

    public double getRatio() {
        return ratio;
    }
}
