package org.example.shared;

/**
 * The class represents an entry of the local storage.
 */
public class Entry {
    /** String name of the data item. */
    public String value;
    /** version of the data item. */
    public int version;

    public Entry (String value, int version) {
        this.value = value;
        this.version = version;
    }

    @Override
    public boolean equals(Object other) {
        if (other==null) return false;
        if (other.getClass() != this.getClass()) return false;
        return this.value.equals(((Entry)other).value) 
        && this.version == ((Entry)other).version;
    }
}
