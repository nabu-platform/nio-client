package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.SSLContext;

import be.nabu.libs.events.api.EventDispatcher;
import be.nabu.libs.nio.api.NIOClient;
import be.nabu.libs.nio.api.NIOConnector;
import be.nabu.libs.nio.api.Pipeline;
import be.nabu.libs.nio.api.PipelineFactory;
import be.nabu.libs.nio.api.PipelineWithMetaData;

public class NIOClientImpl extends NIOServerImpl implements NIOClient {

	private boolean keepAlive = true;
	private volatile boolean started;
	private NIOConnector connector;
	private List<Runnable> runnables = Collections.synchronizedList(new ArrayList<Runnable>());
	private Map<SocketChannel, PipelineFuture> futures = Collections.synchronizedMap(new HashMap<SocketChannel, PipelineFuture>());
	private List<SocketChannel> finalizers = Collections.synchronizedList(new ArrayList<SocketChannel>());

	public NIOClientImpl(SSLContext sslContext, int ioPoolSize, int processPoolSize, PipelineFactory pipelineFactory, EventDispatcher dispatcher, ThreadFactory threadFactory) {
		super(sslContext, null, 0, ioPoolSize, processPoolSize, pipelineFactory, dispatcher, threadFactory);
		if (ioPoolSize < 2) {
			throw new IllegalArgumentException("The IO pool size is not big enough to finalize connections");
		}
	}
	
	@Override
	public Future<Pipeline> connect(final String host, final Integer port) throws IOException {
		if (!started) {
			throw new IllegalStateException("The client must be started before connections can be created");
		}
		final PipelineFuture future = new PipelineFuture();
		// the selector register action _must_ occur in the same thread as the one listening, otherwise deadlocks can occur
		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				try {
					if (!future.isCancelled()) {
						logger.debug("Connecting to {}:{}", host, port);
						SocketChannel channel = getConnector().connect(NIOClientImpl.this, host, port);
						// this will send a TCP level keep alive message
						// the interval and other settings are OS-specific
						// note: it may not be supported on all operating systems
						if (keepAlive) {
							try {
								channel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
							}
							catch (Exception e) {
								logger.warn("Failed to set SO_KEEPALIVE", e);
							}
						}
						futures.put(channel, future);
						SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
					    Pipeline pipeline = getPipelineFactory().newPipeline(NIOClientImpl.this, key);
					    synchronized (channels) {
					    	channels.put(channel, pipeline);
					    }
					    if (pipeline instanceof PipelineWithMetaData) {
					    	((PipelineWithMetaData) pipeline).getMetaData().put("host", host);
					    	((PipelineWithMetaData) pipeline).getMetaData().put("port", port);
					    }
					    getConnector().tunnel(NIOClientImpl.this, host, port, pipeline);
					    future.setResponse(pipeline);
					}
				}
				catch (Exception e) {
					future.cancel(true);
					throw new RuntimeException(e);
				}
			}
		};
		runnables.add(runnable);
		selector.wakeup();
		return future;
	}

	@Override
	public void start() throws IOException {
		selector = Selector.open();

		started = true;
		while (started) {
			selector.select(10000);
			if (started) {
				List<Runnable> runnables = new ArrayList<Runnable>(this.runnables);
				this.runnables.removeAll(runnables);
				for (Runnable callable : runnables) {
					try {
						callable.run();
					}
					catch (Exception e) {
						logger.error("Could not run outstanding runnable", e);
					}
				}
				Set<SelectionKey> selectedKeys = selector.selectedKeys();
	        	Iterator<SelectionKey> iterator = selectedKeys.iterator();
	        	while (iterator.hasNext()) {
	        		SelectionKey key = iterator.next();
	        		final SocketChannel clientChannel = (SocketChannel) key.channel();
	        		if (!clientChannel.isConnected() && key.isConnectable()) {
	        			if (!finalizers.contains(clientChannel)) {
	        				finalizers.add(clientChannel);
		        			submitIOTask(new Runnable() {
		        				public void run() {
		        					PipelineFuture pipelineFuture = futures.get(clientChannel);
		        					if (pipelineFuture == null) {
		        						logger.warn("Unknown channel: " + clientChannel);
		        						try {
		        							clientChannel.close();
		        						}
		        						catch (Exception e) {
		        							logger.warn("Could not close unknown channel", e);
		        						}
		        					}
		        					else {
			        					try {
			        						if (pipelineFuture.isCancelled()) {
				        						// cancel again to make sure we close any remaining pipelines
				        						pipelineFuture.cancel(true);
				        					}
			        						else {
				        						logger.debug("Finalizing accepted connection to: {}", clientChannel.getRemoteAddress());
						        				// finalize the connection
						            			while (clientChannel.isConnectionPending() || !clientChannel.finishConnect()) {
						            				clientChannel.finishConnect();
						            			}
					            				logger.debug("Realizing {}", pipelineFuture);
					            				pipelineFuture.unstage();
			        						}
			        					}
			        					catch (Exception e) {
			        						logger.warn("Could not finalize connection", e);
			        						pipelineFuture.fail(e);
			        					}
			        					finally {
			        						futures.remove(clientChannel);
			        						finalizers.remove(clientChannel);
			        					}
		        					}
		        				}
		        			});
	        			}
	        		}
	        		else if (!channels.containsKey(clientChannel)) {
	        			logger.warn("No channel, cancelling key for: {}", clientChannel.socket());
	        			close(key);
	        		}
	        		else if (!clientChannel.isConnected() || !clientChannel.isOpen() || clientChannel.socket().isInputShutdown()) {
	        			logger.warn("Disconnected, cancelling key for: {}", clientChannel.socket());
	    				Pipeline pipeline = channels.get(clientChannel);
	    				if (pipeline != null) {
							pipeline.close();
	    				}
	    				else {
	    					close(key);
	    				}
	    				PipelineFuture remove = futures.remove(clientChannel);
	    				if (remove != null) {
	    					remove.cancel(true);
	    				}
	        		}
	        		else {
	        			Pipeline pipeline = channels.get(clientChannel);
	    				if (key.isReadable() && pipeline != null) {
	    					logger.trace("Scheduling pipeline, new data for: {}", clientChannel.socket());
	    					pipeline.read();
	    				}
	        			if (key.isWritable() && pipeline != null) {
	        				logger.trace("Scheduling write processor, write buffer available for: {}", clientChannel.socket());
	    					pipeline.write();
	        			}
	    			}
	        		if (lastPrune == null || new Date().getTime() - lastPrune.getTime() > pruneInterval) {
	            		pruneConnections();
	            		lastPrune = new Date();
	            	}
	        	}
			}
		}
	}

	public void pruneConnections() {
		super.pruneConnections();
		lastPrune = new Date();
	}
	
	@Override
	public void stop() {
		started = false;
		closePipelines();
		// make sure we trigger the selector
		selector.wakeup();
	}
	
	public NIOConnector getConnector() {
		if (connector == null) {
			connector = new NIODirectConnector();
		}
		return connector;
	}

	public void setConnector(NIOConnector connector) {
		this.connector = connector;
	}
	
	public class PipelineFuture implements Future<Pipeline> {

		private Pipeline response, stage;
		private CountDownLatch latch = new CountDownLatch(1);
		private Throwable exception;
		private boolean cancelled;
		
		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			if (response != null) {
				try {
					response.close();
				}
				catch (Exception e) {
					// ignore
					logger.debug("Could not close cancelled pipeline future", e);
				}
				response = null;
			}
			if (stage != null) {
				try {
					stage.close();
				}
				catch (Exception e) {
					// ignore
					logger.debug("Could not close cancelled pipeline future", e);
				}
				stage = null;
			}
			cancelled = true;
			latch.countDown();
			return cancelled;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public boolean isDone() {
			return latch.getCount() == 0 && response != null;
		}

		@Override
		public Pipeline get() throws InterruptedException, ExecutionException {
			try {
				return get(365, TimeUnit.DAYS);
			}
			catch (TimeoutException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Pipeline get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
			if (latch.await(timeout, unit)) {
				if (response == null) {
					throw new ExecutionException("No response found", exception);
				}
				else {
					return response;
				}
            }
			else {
                throw new TimeoutException();
        	}
		}

		public Pipeline getResponse() {
			return response;
		}

		public void setResponse(Pipeline response) {
			if (this.stage != null) {
				throw new IllegalStateException("A response has already been set");
			}
			this.stage = response;
		}
		
		public void unstage() throws IOException {
			if (this.stage == null) {
				throw new IllegalStateException("No staged value");
			}
			try {
				if (((MessagePipelineImpl<?, ?>) stage).isUseSsl()) {
					Future<?> handshake = ((MessagePipelineImpl<?, ?>) stage).startHandshake();
					handshake.get(30, TimeUnit.SECONDS);
				}
				this.response = this.stage;
			}
			catch (Exception e) {
				this.exception = e;
			}
			latch.countDown();
		}
		
		public void fail(Throwable exception) {
			this.exception = exception;
			cancel(true);
		}
		
		public String toString() {
			return "pipelineFuture to: " + (stage == null ? "unstaged" : stage.getSourceContext().getSocketAddress());
		}
	}

	public boolean isStarted() {
		return started;
	}
	
}

