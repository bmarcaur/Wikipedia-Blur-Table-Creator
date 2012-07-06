package parsers;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.TreeMap;

import models.Revision;

public class WikipediaDataParser {
	private static String[] namespaces = {"Main", "Talk", "User", "User Talk", "Wikipedia", "Wikipedia Talk"};
	private static String[] categories = {"None", "Deletion", "Mediation", "Featured Lists", "Featured Pictures", "Arbitration", "Featured Topic", "Featured Portal", "Featured Article", "Featured Sounds", "Good Article"};
	private static TreeMap<String, LinkedList<String[]>> revisionData = new TreeMap<String, LinkedList<String>>();
	
	public static TreeMap<String, LinkedList<String[]>> parseRevisions() {
		try {
			BufferedReader revisionReader = new BufferedReader(new FileReader("training.tsv"));
			// Discard the Schema Line
			revisionReader.readLine();
			// Buffer for the line in the file
			String returnedLine;
			while((returnedLine = revisionReader.readLine()) != null){
				// Grab the first "column" which is the user id
				String[] parsedRevisionInfo = returnedLine.split("\\t");
				String userId = parsedRevisionInfo[0];
				// Add the revision object to the map
				if (revisionData.containsKey(userId)){
					revisionData.get(userId).add(parsedRevisionInfo);
				} else {
					LinkedList<String[]> newRevisionList = new LinkedList<String[]>();
					newRevisionList.add(parsedRevisionInfo);
					revisionData.put(userId, newRevisionList);
				}
			}
			revisionReader.close();
		} catch (FileNotFoundException e) {
			System.err.print("File was not found");
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e){
			System.err.print("A problem occurred while trying to read from the buffered stream.");
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static TreeMap<String, ArrayList<String>> parseComments() {

	}

	public static TreeMap<String, ArrayList<String>> parseArticleInformation() {

	}
}
