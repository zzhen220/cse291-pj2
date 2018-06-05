package surfstore;

import surfstore.SurfStoreBasic.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

import java.io.File;

public class test {

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("test").build()
                .description("test for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    private static void ensure(boolean b){
        if (!b) {
            throw new RuntimeException("Assertion failed!");
        }
    }

    private static final String filePath = "../test/test.txt";
    private static final String fileName = "test.txt";
    private static final String dowloadPath = "../";
    public static void main(String[] args) throws Exception{
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);
        Client client = new Client(config);

        MetadataStoreGrpc.MetadataStoreBlockingStub follower1 = MetadataStoreGrpc.newBlockingStub(
                ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                .usePlaintext(true).build());

        MetadataStoreGrpc.MetadataStoreBlockingStub follower2 = MetadataStoreGrpc.newBlockingStub(
                ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
                        .usePlaintext(true).build());

        try {
            ensure(client.getVersion(fileName) == 0);
            client.upLoad(filePath);
            client.delete(fileName);
            ensure(client.getVersion(fileName) == 2);
            follower1.crash(SurfStoreBasic.Empty.newBuilder().build());
            ensure(follower1.isCrashed(Empty.newBuilder().build()).getAnswer());
            client.upLoad(filePath);
            client.delete(fileName);
            ensure(client.getVersion(fileName) == 4);
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 2);
            ensure(follower2.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 4);
            follower1.restore(Empty.newBuilder().build());
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 4);
            client.upLoad(filePath);
            client.delete(fileName);
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 6);
            ensure(follower2.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 6);
            follower2.crash(Empty.newBuilder().build());
            ensure(follower2.isCrashed(Empty.newBuilder().build()).getAnswer());
            client.upLoad(filePath);
            client.delete(fileName);
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 8);
            ensure(follower2.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 6);
            follower2.restore(Empty.newBuilder().build());
            ensure(follower2.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 8);
            follower1.crash(SurfStoreBasic.Empty.newBuilder().build());
            ensure(follower1.isCrashed(Empty.newBuilder().build()).getAnswer());
            client.upLoad(filePath);
            client.delete(fileName);
            ensure(client.getVersion(fileName) == 10);
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 8);
            ensure(follower2.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 10);
            follower1.restore(Empty.newBuilder().build());
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 10);
            follower1.crash(SurfStoreBasic.Empty.newBuilder().build());
            ensure(follower1.isCrashed(Empty.newBuilder().build()).getAnswer());
            client.upLoad(filePath);
            client.delete(fileName);
            ensure(client.getVersion(fileName) == 12);
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 10);
            ensure(follower2.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 12);
            follower1.restore(Empty.newBuilder().build());
            ensure(follower1.getVersion(FileInfo.newBuilder().setFilename(fileName).build()).getVersion() == 12);
        } finally {
            client.shutdown();
        }
    }
}
