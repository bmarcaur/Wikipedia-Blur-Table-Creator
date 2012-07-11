package processes;

import java.util.ArrayList;
import java.util.TreeMap;

import parsers.*;
import loaders.*;

public class WikipediaDataProcess {	
	private static String[] namespaces = {"Main", "Talk", "User", "User Talk", "Wikipedia", "Wikipedia Talk"};
	private static String[] categories = {"None", "Deletion", "Mediation", "Featured Lists", "Featured Pictures", "Arbitration", "Featured Topic", "Featured Portal", "Featured Article", "Featured Sounds", "Good Article"};
	
	public static void main(String[] args) {
		// Load in all of the files using buffered input streams
		TreeMap<String, ArrayList<ArrayList<String>>> revisions = WikipediaDataParser.parseRevisions();
		// TreeMap<String, String[]> comments = WikipediaDataParser.parseComments();
		// TreeMap<String, String[]> articleInformation = WikipediaDataParser.parseArticleInformation();
	}
}
