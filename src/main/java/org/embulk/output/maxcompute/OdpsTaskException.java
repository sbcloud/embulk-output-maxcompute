package org.embulk.output.maxcompute;

public class OdpsTaskException extends Exception{

    public OdpsTaskException(String message) {
        super(message);
    }

    public OdpsTaskException(String message, Throwable cause) {
        super(message, cause);
    }
}
