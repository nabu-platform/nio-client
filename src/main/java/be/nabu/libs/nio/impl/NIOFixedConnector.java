package be.nabu.libs.nio.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import be.nabu.libs.nio.api.NIOClient;
import be.nabu.libs.nio.api.NIOConnector;
import be.nabu.libs.nio.api.Pipeline;

public class NIOFixedConnector implements NIOConnector {

	private int fixedPort;
	private String fixedHost;

	public NIOFixedConnector(String fixedHost, int fixedPort) {
		this.fixedHost = fixedHost;
		this.fixedPort = fixedPort;
	}
	
	@Override
	public SocketChannel connect(NIOClient client, String host, Integer port) throws IOException {
		InetSocketAddress serverAddress = new InetSocketAddress(fixedHost, fixedPort);
		SocketChannel channel = SocketChannel.open();
	    channel.configureBlocking(false);
	    channel.connect(serverAddress);
	    return channel;
	}

	@Override
	public void tunnel(NIOClient client, String host, Integer port, Pipeline pipeline) throws IOException {
		// do nothing
	}

}
