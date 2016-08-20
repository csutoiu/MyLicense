package streaming;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

public class EventServer {

	private final Executor SERVER = Executors.newSingleThreadExecutor();
	private final static int PORT = 10001;
	private static final long EVENT_PERIOD_SECONDS = 5000;
	private BlockingQueue<String> fileEvents;
	
	public EventServer(BlockingQueue<String> fileEvents) {
		this.fileEvents = fileEvents;
	}
	
	public void sendEvents() {
		SERVER.execute(new StreamingServer(fileEvents));
	}

	private static class StreamingServer implements Runnable {

		private BlockingQueue<String> fileEvents;
	
		public StreamingServer(BlockingQueue<String> fileEvents) {
			this.fileEvents = fileEvents;
		}
	
		public void run() {
			try {
				ServerSocket serverSocket = new ServerSocket(PORT);
				Socket clientSocket = serverSocket.accept();
				PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
				
				while(!fileEvents.isEmpty()) {
					String event = fileEvents.take();
					out.println(event);
					Thread.sleep(EVENT_PERIOD_SECONDS);
				}
				serverSocket.close();
				clientSocket.close();
			} catch (IOException e) {
				throw new RuntimeException("Server error", e);
			} catch (InterruptedException e) {
				throw new RuntimeException("Server error", e);
			}		
		}
	}
}