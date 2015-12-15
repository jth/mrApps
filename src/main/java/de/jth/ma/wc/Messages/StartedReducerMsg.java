package de.jth.ma.wc.Messages;

/**
 * TODO: Put this to use instead of static ports etc.
 * Created by jth on 12/14/15.
 */
public class StartedReducerMsg {
    public final String TYPE = "StartReducerMessage";
    public final int id;
    public final int port;
    public final String host;

    public StartedReducerMsg(String host, int port, int id) {
        this.host = host;
        this.port = port;
        this.id = id;
    }
}
