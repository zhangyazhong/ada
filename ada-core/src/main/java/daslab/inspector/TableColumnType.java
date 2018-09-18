package daslab.inspector;

/**
 * @author zyz
 * @version 2018-05-14
 */
public enum TableColumnType {
    INT("int"), STRING("string"), DOUBLE("double"), DATE("date");

    private String tag;

    TableColumnType(String tag) {
        this.tag = tag;
    }

    public static TableColumnType getType(String tag) {
        for (TableColumnType type : TableColumnType.values()) {
            if (type.getTag().equals(tag.toLowerCase())) {
                return type;
            }
        }
        return null;
    }

    public String getTag() {
        return tag;
    }

    public boolean isInt() {
        return this.tag.equals(INT.getTag());
    }

    public boolean isString() {
        return this.tag.equals(STRING.getTag());
    }

    public boolean isDouble() {
        return this.tag.equals(DOUBLE.getTag());
    }

    public boolean isDate() {
        return this.tag.equals(DATE.getTag());
    }

    @Override
    public String toString() {
        return this.tag;
    }

}
