package com.flipkart.redis.net;

public class ConnectionFactory {
	
	private static ConnectionFactory instance = null;
	
	public static ConnectionFactory getInstance()
    {
        if(instance == null)
        {
            instance = new ConnectionFactory();
        }
        return  instance;
    }
	
	public Connection create(String host, int port, String password, int soTimeout)
	{
		Connection connection = new Connection(host, port);
		connection.setSoTimeout(soTimeout);
		connection.connect();
		
		if(password != null && !password.isEmpty()) {
			connection.authenticate(password);
		}
		
		return connection;
	}
	
	public SlaveConnection createAsSlave(String host, int port, String password, int soTimeout, long replicationOffset)
	{
		SlaveConnection connection = new SlaveConnection(host, port);
		
		connection.setSoTimeout(soTimeout);
		connection.connect();
		
		if(password != null && !password.isEmpty()) {
			connection.authenticate(password);
		}
		
		connection.requestForPSync(replicationOffset);
		
		return connection;
	}
}
