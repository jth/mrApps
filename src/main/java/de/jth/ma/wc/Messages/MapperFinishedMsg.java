package de.jth.ma.wc.Messages;

/**
 * Message that gets send from mapper in order to get the
 * port on where to pull the data from.
 * Created by jth on 12/14/15.
 */
public class MapperFinishedMsg {
    public final String TYPE = "MapperFinishedMessage";
    public final int id;
    public final int port;
    public final String hostname;

    public MapperFinishedMsg(int id, int port, String hostname) {
        this.port = port;
        this.hostname = hostname;
        this.id = id;
    }

    @Override
    public String toString() {
        return "MapperFinishedMsg: " + hostname + ":" + port + ", id=" + id;
    }
}
