package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.*;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());

    protected Server server;
	protected ConfigReader config;

    public MetadataStore(ConfigReader config) {
    	this.config = config;
	}

	private void start(int port, int numThreads, boolean isLeader) throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(new MetadataStoreImpl(this.config.blockPort, isLeader))
                .executor(Executors.newFixedThreadPool(numThreads))
                .build()
                .start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class)
                .help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1)
                .help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

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

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }

        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"),
                config.getLeaderNum() == c_args.getInt("number"));
        server.blockUntilShutdown();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        Map<String, Integer> file_versionMap;
        Map<String, List<String>> file_blocklistMap;
        private final boolean leader;
        private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;
        private boolean crashed;

        MetadataStoreImpl(int blockPort, boolean isLeader){
            super();
            blockStub=BlockStoreGrpc.newBlockingStub(ManagedChannelBuilder.forAddress("127.0.0.1", blockPort)
                    .usePlaintext(true).build());
            file_versionMap=new HashMap<>();
            file_blocklistMap=new HashMap<>();
            this.leader = isLeader;
            this.crashed = false;
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        // TODO: Implement the other RPCs!

        /**
         *Read the requested file.
         * @param request Client supply filename of FileInfo
         * @param responseObserver
         */
        @Override
        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            FileInfo.Builder builder=FileInfo.newBuilder();

            logger.info("file name: " + request.getFilename());
            //file never exist
            if(!file_versionMap.containsKey(request.getFilename())){
                builder.setVersion(0);
            }
            //file has been deleted
            else if(file_blocklistMap.containsKey(request.getFilename())&&
                    file_blocklistMap.get(request.getFilename()).size()==1&&
                    file_blocklistMap.get(request.getFilename()).get(0).equals("0")){
                builder.setVersion(file_versionMap.get(request.getFilename()));
                builder.addAllBlocklist(file_blocklistMap.get(request.getFilename()));
            }
            //file exist
            else{
                builder.setVersion(file_versionMap.get(request.getFilename()));
                builder.addAllBlocklist(file_blocklistMap.get(request.getFilename()));
            }
            FileInfo response=builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * Write a file
         * @param request
         * @param responseObserver
         */
        @Override
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver){
            WriteResult.Builder builder=WriteResult.newBuilder();

            //version wrong
            int version = file_versionMap.getOrDefault(request.getFilename(), 0);
            builder.setCurrentVersion(version);
            if(request.getVersion() != version+1)
                builder.setResultValue(1);
            else {
                List<String> missing_block=new ArrayList<>();
                for(int i=0;i<request.getBlocklistList().size();i++){
                    String hash=request.getBlocklist(i);
                    Block block_checking=Block.newBuilder().setHash(hash).build();
                    SimpleAnswer ans=blockStub.hasBlock(block_checking);
                    if(!ans.getAnswer()){
                        missing_block.add(hash);
                    }
                }

                //missing block
                if(missing_block.size()!=0){
                    builder.setResultValue(2);
                    builder.addAllMissingBlocks(missing_block);
                }
                else{
                    //ok
                    logger.info("Metadata store modification successful. New version number is" + request.getVersion());
                    builder.setResultValue(0);
                    builder.setCurrentVersion(request.getVersion());
                    file_versionMap.put(request.getFilename(),request.getVersion());
                    file_blocklistMap.put(request.getFilename(),request.getBlocklistList());
                }
            }

            WriteResult response=builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * Delete a file
         * @param request
         * @param responseObserver
         */
        @Override
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver){
            WriteResult.Builder builder=WriteResult.newBuilder();
            //version wrong
            if(file_versionMap.containsKey(request.getFilename())) {
                int version = file_versionMap.get(request.getFilename());
                builder.setCurrentVersion(version);
                if (request.getVersion() != version + 1)
                    builder.setResultValue(1);
                else {
                    builder.setResultValue(0);
                    builder.setCurrentVersion(request.getVersion());
                    file_versionMap.put(request.getFilename(),request.getVersion());
                    List<String> temp=new ArrayList<>();
                    temp.add("0");
                    file_blocklistMap.put(request.getFilename(),temp);
                }
            }

            WriteResult response=builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted(); //why don't we delete blocks in blockstore?
        }

        /**
         * Query whether the MetadataStore server is currently the leader.
         * @param request
         * @param responseObserver
         */
        @Override
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                             io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.leader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void crash(surfstore.SurfStoreBasic.Empty request,
                          io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            if(!this.leader)
                throw new RuntimeException("crash on leader machine");
            this.crashed = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void restore(surfstore.SurfStoreBasic.Empty request,
                            io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            if(!this.leader)
                this.crashed = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                              io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(this.crashed).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        @Override
        public void getVersion(surfstore.SurfStoreBasic.FileInfo request,
                               io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setVersion(file_versionMap.getOrDefault(request.getFilename(), 0));
            responseObserver.onNext(builder.build());
            responseObserver.onCompleted();
        }
    }
}