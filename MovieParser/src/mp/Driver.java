package mp;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class Driver {

	// Open up input.txt of user ratings outputted by MapReduce
	public static void main(String[] args) {
		ArrayList<Tuple> list = new ArrayList<Tuple>();
		try {
			File f = new File(args[0]);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();
			// Dump all entries into a Tuple array list
			while (line != null) {
				String[] arr = line.split("\\t");
				list.add(new Tuple(Integer.parseInt(arr[0]), Double.parseDouble(arr[1])));
				line = br.readLine();
			}
			br.close();
			fr.close();

			// Sort the tuples
			Collections.sort(list, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple t1, Tuple t2) {
					double num = (t1.score - t2.score);
					if (num > 0) {
						return 1;
					} else if (num == 0) {
						return 0;
					} else {
						return -1;
					}
				}
			});

			// Only save top 10
			Tuple[] movies = new Tuple[10];

			for (int i = 0; i < 10; i++) {
				Tuple t = list.get(list.size() - 1 - i);
				movies[i] = t;
			}

			ArrayList<String> names = loadMovieNames(args[1]);
			for (Tuple t : movies) {
				System.out.println("ID # " + t.movieID + " | Title: " + names.get(t.movieID - 1) + " | Score #" + t.score);
			}

		} catch (Exception e) {
			System.out.println("Requires Map-Reduce Ratings output and movie data");
		}
	}

	// Load all movie names.
	private static ArrayList<String> loadMovieNames(String fileName) {
		ArrayList<String> list = new ArrayList<String>();
		try {
			File f = new File(fileName);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			String line = br.readLine();

			while (line != null) {
				String id = line.substring(0, line.indexOf(','));
				list.add(rSplit(line, ','));					
				line = br.readLine();
			}
			br.close();
			fr.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return list;
	}

	// Remove everything before first indexof character c, twice
	private static String rSplit(String str, char c) {
		String s = str;
		s = s.substring(s.indexOf(c) + 1);
		s = s.substring(s.indexOf(c) + 1);
		return s;
	}

	private static class Tuple {
		int movieID;
		double score;

		public Tuple(int id, double num) {
			movieID = id;
			score = num;
		}
	}
}
