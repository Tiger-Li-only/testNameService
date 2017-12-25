import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class IdMaker {
	
	
	private ZkClient client = null;
	private final String server;
	private final String root;
	private final String nodeName;
	private volatile boolean running = false;
	private ExecutorService cleanExector = null;

    /**
     * 删除类型枚举，不删除，立即删除，延迟删除
     */
	public enum RemoveMethod{
		NONE,IMMEDIATELY,DELAY
		
	}
	
	public IdMaker(String zkServer,String root,String nodeName){
		
		this.root = root;
		this.server = zkServer;
		this.nodeName = nodeName;
		
	}
	
	public void start() throws Exception {
		
		if (running)
			throw new Exception("server has stated...");
		running = true;
		
		init();
		
	}
	
	
	public void stop() throws Exception {
		
		if (!running)
			throw new Exception("server has stopped...");
		running = false;
		
		freeResource();
		
	}
	
	
	private void init(){
		
		client = new ZkClient(server,5000,5000,new BytesPushThroughSerializer());
		cleanExector = Executors.newFixedThreadPool(10);
		try{
			client.createPersistent(root,true);
		}catch (ZkNodeExistsException e){
			//ignore;
		}
		
	}
	
	private void freeResource(){
//        将线程池状态置为SHUTDOWN,并不会立即停止：
//
//        停止接收外部submit的任务
//        内部正在跑的任务和队列里等待的任务，会执行完
//        等到第二步完成后，才真正停止
		cleanExector.shutdown();
		try{
//            当前线程阻塞，直到
//
//            等所有已提交的任务（包括正在跑的和队列中等待的）执行完
//                    或者等超时时间到
//            或者线程被中断，抛出InterruptedException
//            然后返回true（shutdown请求后所有任务执行完毕）或false（已超时）
//
//            实验发现，shuntdown()和awaitTermination()效果差不多，方法执行之后，都要等到提交的任务全部执行完才停。
			cleanExector.awaitTermination(2, TimeUnit.SECONDS);
			
		}catch(InterruptedException e){
			e.printStackTrace();
		}finally{
			cleanExector = null;
		}
	
		if (client!=null){
			client.close();
			client=null;
			
		}
	}
	
	private void checkRunning() throws Exception {
		if (!running)
			throw new Exception("请先调用start");
		
	}
	
	private String ExtractId(String str){
		int index = str.lastIndexOf(nodeName);
		if (index >= 0){
			index+=nodeName.length();
			return index <= str.length()?str.substring(index):"";
		}
		return str;
		
	}
	
	public String generateId(RemoveMethod removeMethod) throws Exception{
		checkRunning();
		final String fullNodePath = root.concat("/").concat(nodeName);
		final String ourPath = client.createPersistentSequential(fullNodePath, null);
		
		if (removeMethod.equals(RemoveMethod.IMMEDIATELY)){
			client.delete(ourPath);
		}else if (removeMethod.equals(RemoveMethod.DELAY)){
			cleanExector.execute(new Runnable() {
				
				public void run() {
					// TODO Auto-generated method stub
					client.delete(ourPath);
				}
			});
			
		}
		//node-0000000000, node-0000000001
		return ExtractId(ourPath);
	}

}
