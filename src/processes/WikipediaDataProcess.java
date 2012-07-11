package processes;

import java.util.ArrayList;
import java.util.TreeMap;

import parsers.*;
import loaders.*;

public class WikipediaDataProcess {	
	public static void main(String[] args) {
		// Load in all of the files using buffered input streams
		TreeMap<String, ArrayList<String>> revisions = WikipediaDataParser.parseRevisions();
		TreeMap<String, ArrayList<String>> comments = WikipediaDataParser.parseComments();
		TreeMap<String, ArrayList<String>> articleInformation = WikipediaDataParser.parseArticleInformation();
	}
}
