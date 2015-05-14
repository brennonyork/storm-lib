package org.brennonyork.poseidon.accumulo;

import java.io.Serializable;

import java.util.HashMap;
import java.util.Map;

/**
 * The AccumuloConfig object handles all parameters related to connection and execution
 * of client calls.
 *
 * Currently the only mandated fields that must be filled in for a connection to be
 * established are:</br></br>
 * <ul><li><b>ZOOKEEPER_INSTANCE_NAME</b></li>
 *     <li><b>ZOOKEEPER_SERVER_LIST</b></li>
 *     <li><b>ACCUMULO_USER</b></li>
 *     <li><b>ACCUMULO_PASSWORD</b></li>
 * </ul></br>
 * Once each of these values are declared a session can be established and client 
 * (i.e. remote) execution can take place.</br></br>
 *
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York 
 */
public class AccumuloConfig extends HashMap<String,Object> {
    /**
     * Name of the Zookeeper instance associated with the Accumulo cluster.
     */
    public static String ZOOKEEPER_INSTANCE_NAME = "zookeeper.instance.name";

    /**
     * Set of Zookeeper servers associated with the given Accumulo cluster.
     * This can be a single machine or set of machines as a String of
     * comma-delineated hosts.
     */
    public static String ZOOKEEPER_SERVER_LIST = "zookeeper.server.list";

    /**
     * Accumulo user to access the cluster as.
     */
    public static String ACCUMULO_USER = "accumulo.user";

    /**
     * The password for the specified Accumulo user.
     * @see #ACCUMULO_USER
     */
    public static String ACCUMULO_PASSWORD = "accumulo.password";

    public static void setZookeeperInstName(Map conf, String zkInstName) {
	conf.put(AccumuloConfig.ZOOKEEPER_INSTANCE_NAME, zkInstName);
    }

    public void setZookeeperInstName(String zkInstName) {
	setZookeeperInstName(this, zkInstName);
    }

    public static void setZookeeperServers(Map conf, String zkServers) {
	conf.put(AccumuloConfig.ZOOKEEPER_SERVER_LIST, zkServers);
    }

    public void setZookeeperServers(String zkServers) {
	setZookeeperServers(this, zkServers);
    }

    public static void setAccumuloUser(Map conf, String accumuloUser) {
	conf.put(AccumuloConfig.ACCUMULO_USER, accumuloUser);
    }

    public void setAccumuloUser(String accumuloUser) {
	setAccumuloUser(this, accumuloUser);
    }

    public static void setAccumuloPassword(Map conf, String accumuloPasswd) {
	conf.put(AccumuloConfig.ACCUMULO_PASSWORD, accumuloPasswd);
    }

    public void setAccumuloPassword(String accumuloPasswd) {
	setAccumuloPassword(this, accumuloPasswd);
    }
}
