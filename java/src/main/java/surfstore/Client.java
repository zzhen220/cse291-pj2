package surfstore;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.List;
import java.util.ArrayList;


import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;


public final class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel metadataChannel;
    private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

    private final ManagedChannel blockChannel;
    private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

    private final ConfigReader config;
    private Map<String, byte[]> hash_to_data;

    public Client(ConfigReader config) {
        this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                .usePlaintext(true).build();
        this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

        this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort())
                .usePlaintext(true).build();
        this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        this.config = config;

        this.hash_to_data = new HashMap<>();
    }

    public void shutdown() throws InterruptedException {
        metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void download(String fileName, String downPath) throws NoSuchFileException{
        FileInfo.Builder fileinfo_builder=FileInfo.newBuilder();
        fileinfo_builder.setFilename(fileName);
        FileInfo readfile_res=metadataStub.readFile(fileinfo_builder.build());
        if(readfile_res.getVersion() == 0||
                (readfile_res.getBlocklistCount() == 1&&readfile_res.getBlocklist(0).equals("0"))){
            throw new NoSuchFileException(fileName);
        }
        List<String> missing_hashes=new ArrayList<>();
        List<String> all_hashes=readfile_res.getBlocklistList();
        for(int i=0;i<all_hashes.size();i++){
            if(hash_to_data.containsKey(all_hashes.get(i))) continue;
            missing_hashes.add(all_hashes.get(i));
        }
        for(int i=0;i<missing_hashes.size();i++){
            Block.Builder block_builder=Block.newBuilder();
            block_builder.setHash(missing_hashes.get(i));
            Block block=block_builder.build();
            SimpleAnswer ans=blockStub.hasBlock(block);
            if(ans.getAnswer()){
                byte[] arr=blockStub.getBlock(block).getData().toByteArray();
                hash_to_data.put(missing_hashes.get(i),arr); //store missing blocks in the map with (hash, byte[]) pair
            }
        }

        //Write file to disk
        File f = new File(downPath+"/"+fileName);
        OutputStream os = null;
        try {
            os = new FileOutputStream(f);
            for(String s : all_hashes) {
                os.write(hash_to_data.get(s));
            }
            os.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void upLoad(String path) throws IOException{
        List<String> hash_list=HashUtils.compute_hashlist(path, hash_to_data);
        FileInfo.Builder fileinfo_builder=FileInfo.newBuilder();
        fileinfo_builder.setFilename(Paths.get(path).getFileName().toString());
        FileInfo readfile_res=metadataStub.readFile(fileinfo_builder.build());
        fileinfo_builder.setVersion(readfile_res.getVersion()+1);
        fileinfo_builder.addAllBlocklist(hash_list);
        WriteResult modify_res=metadataStub.modifyFile(fileinfo_builder.build());

        while (modify_res.getResultValue() != 0){
            if(modify_res.getResultValue() == 1){
                fileinfo_builder.setVersion(modify_res.getCurrentVersion()+1);
            } else if(modify_res.getResultValue()==2){
                List<String> missing_blocks=modify_res.getMissingBlocksList();
                for(int i=0;i<missing_blocks.size();i++){
                    Block.Builder block_builder=Block.newBuilder();
                    block_builder.setHash(missing_blocks.get(i)); //get missing block hashlist
                    block_builder.setData(ByteString.copyFrom(hash_to_data.get(missing_blocks.get(i)))); //get missing block data
                    blockStub.storeBlock(block_builder.build());
                }
                fileinfo_builder.setVersion(modify_res.getCurrentVersion()+1);
            }
            modify_res=metadataStub.modifyFile(fileinfo_builder.build());
        }

    }

    public void delete(String path) throws NoSuchFileException{
        FileInfo.Builder fileinfo_builder=FileInfo.newBuilder();
        fileinfo_builder.setFilename(path);
        FileInfo readfile_res=metadataStub.readFile(fileinfo_builder.build());
        if((readfile_res.getBlocklistCount() == 1&&
                readfile_res.getBlocklist(0).equals("0"))||
                readfile_res.getVersion() == 0)
            throw new NoSuchFileException(path);
        fileinfo_builder.setVersion(readfile_res.getVersion()+1);
        WriteResult modify_res=metadataStub.deleteFile(fileinfo_builder.build());
        //do something else????
    }

    public int getVersion(String path) {
        FileInfo.Builder fileinfo_builder=FileInfo.newBuilder();
        fileinfo_builder.setFilename(path);
        FileInfo readfile_res=metadataStub.readFile(fileinfo_builder.build());
        System.out.println(readfile_res.getVersion());//print version
        return readfile_res.getVersion();
    }




	private void go(String operation, String filePath, String downPath) {
		metadataStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Metadata server");
        
        blockStub.ping(Empty.newBuilder().build());
        logger.info("Successfully pinged the Blockstore server");

        try {
            switch (operation) {
                case "download":
                    download(filePath, downPath);
                    System.out.println("OK");
                    break;
                case "upload":
                    upLoad(filePath);
                    System.out.println("OK");
                    break;
                case "delete":
                    delete(filePath);
                    System.out.println("OK");
                    break;
                case "getversion":
                    getVersion(filePath);
                    break;
            }
        } catch (IOException e) {
            System.out.println("Not Found");
        }

	}

	/*
	 *
	 */
    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("Client").build()
                .description("Client for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        if(args.length > 1) {
            parser.addArgument("operation").type(String.class)
                    .help("Operation of client");
            parser.addArgument("filePath").type(String.class)
                    .help("Path to client target");
        }
        if (args.length > 3)
            parser.addArgument("downloadDir").type(String.class)
                    .help("Directory of download location");
        
        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e){
            parser.handleError(e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        Namespace c_args = parseArgs(args);
        if (c_args == null){
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        Client client = new Client(config);

        try {
        	client.go(c_args.getString("operation"), c_args.getString("filePath"), c_args.getString("downloadDir"));
        } finally {
            client.shutdown();
        }
    }


    private MetadataStoreGrpc.MetadataStoreBlockingStub crashedFollower;
}
