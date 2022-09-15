package datawave.webservice.common.connection.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.deltaspike.core.api.config.ConfigResolver;

public class ConnectionPoolConfiguration {
    
    private String username;
    private String password;
    private String instance;
    private String zookeepers;
    private int lowPriorityPoolSize;
    private int normalPriorityPoolSize;
    private int highPriorityPoolSize;
    private int adminPriorityPoolSize;
    
    public ConnectionPoolConfiguration(String poolName) {
        username = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.userName");
        password = resolvePassword(poolName);
        instance = ConfigResolver.getPropertyValue("dw." + poolName + ".instanceName");
        zookeepers = ConfigResolver.getPropertyValue("dw." + poolName + ".zookeepers");
        lowPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.low.size", "25"));
        normalPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.normal.size", "50"));
        highPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.high.size", "100"));
        adminPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.admin.size", "200"));
    }
    
    /**
     * Resolve the accumulo password from either the system properties or the environment. The environment takes precedence if both are configured.
     *
     * @param poolName
     *            the name of the connection pool
     * @return the resolved password
     */
    protected String resolvePassword(String poolName) {
        String password = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.password");
        
        String envTarget = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.password.env");
        if (StringUtils.isNotBlank(envTarget)) {
            String envPass = System.getenv(envTarget);
            
            // only set use the value from the environment if it is not blank
            if (StringUtils.isNotBlank(envPass)) {
                password = envPass;
            }
        }
        
        return password;
    }
    
    public String getUsername() {
        return username;
    }
    
    public String getPassword() {
        return password;
    }
    
    public String getInstance() {
        return instance;
    }
    
    public String getZookeepers() {
        return zookeepers;
    }
    
    public int getLowPriorityPoolSize() {
        return lowPriorityPoolSize;
    }
    
    public int getNormalPriorityPoolSize() {
        return normalPriorityPoolSize;
    }
    
    public int getHighPriorityPoolSize() {
        return highPriorityPoolSize;
    }
    
    public int getAdminPriorityPoolSize() {
        return adminPriorityPoolSize;
    }
    
}
