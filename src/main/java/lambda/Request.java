package lambda;

/**
 *
 * @author Wes Lloyd
 */
public class Request {

    private String bucketname;
    private String filename;

    public String getBucketname() {
        return bucketname;
    }

    public void setBucketname(String bucketname) {
        this.bucketname = bucketname;
    }

    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public Request() {

    }
}
