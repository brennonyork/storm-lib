package org.brennonyork.siren;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.File;
import java.io.FileFilter;

import java.util.Map;

/**
 * Handles polling a given directory for files.
 *
 * Handles polling of a given directory for files to be dropped into the 
 * system. When it finds a file, or files, it will move them to a processing
 * directory and emit those absolute file paths into the Storm stream. It polls
 * at a one second interval and, for each item found, passes it through a FileFilter
 * to determine whether or not it is a file (not directory, symlink, etc.). If so
 * it emits it into the stream.</br></br>
 * 
 * <b>OutputFieldsDeclaration</b></br>
 * <table>
 *   <tr><td>abs-proc-path</td>
 *       <td>Absolute path of the file once moved into the processing directory.</td></tr>
 * </table>
 * 
 * <b>Author:</b> <a href="mailto:brennon.york@gmail.com">Brennon York</a>
 * @author Brennon York
 */
public class PollDirectory extends BaseRichSpout {
    private SpoutOutputCollector _collector;
    private FileFilter _fileFilter;
    private File _procDir;
    private File _pollDir;
    private int _fileLen;

    /**
     * Constructor denoting a root directory such that the processing and
     * polling directories reside one level below it at "rootDirectory"/proc
     * and "rootDirectory"/poll respectively.
     *
     * @param rootDirectory Assign a root directory such that polling is done
     *                      from "rootDirectory"/poll and processing within
     *                      "rootDirectory"/proc.
     */
    public PollDirectory(String rootDirectory) { 
	File _rootDir = new File(rootDirectory);
	_pollDir = new File(_rootDir, "poll");
	_procDir = new File(_rootDir, "proc");
    }

    /**
     * Constructor to denote separate polling and processing directories.
     *
     * @param pollDirectory Define a polling directory from which files will
     *                      be monitored for.
     * @param procDirectory Define a processing directory where files successfully
     *                      polled from the pollingDirectory move into. This is the
     *                      absolute path emitted as a tuple.
     */
    public PollDirectory(String pollDirectory, String procDirectory) { 
	_pollDir = new File(pollDirectory);
	_procDir = new File(procDirectory);
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    	_collector = collector;
    	_fileFilter = new DocumentFilter();
    }

    public void nextTuple() {
    	// Returns an array containing all files under the polled directory
	File[] fileList = _pollDir.listFiles(_fileFilter);
	_fileLen = fileList.length;
		
	if(_fileLen > 0) {
	    for(int i = 0; i < _fileLen; ++i) {
		// Move file to a processing directory under the polling directory
		File fileLoc = new File(_procDir, fileList[i].getName());
		fileList[i].renameTo(fileLoc);
				
		// Emit the absolute file path once it arrives in the processing directory
		_collector.emit(new Values(fileLoc.getAbsolutePath()));
	    }
	}
	Utils.sleep(1000); // Sleep for 1 second 
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields("abs-proc-path"));
    }
    
    private class DocumentFilter implements FileFilter {  
    	// Return only items in the directory that are files; disregard other directories, symlinks, etc.
        public boolean accept(File file) {  
            return file.isFile();  
        }  
    } 
}
