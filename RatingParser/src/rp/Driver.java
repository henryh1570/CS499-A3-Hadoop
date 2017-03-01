package rp;

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
				list.add(new Tuple(Integer.parseInt(arr[0]), Integer.parseInt(arr[1])));
				line = br.readLine();
			}
			br.close();
			fr.close();

			// Sort the tuples
			Collections.sort(list, new Comparator<Tuple>() {
				@Override
				public int compare(Tuple t1, Tuple t2) {
					return (t1.reviewNum - t2.reviewNum);
				}
			});
			
			// Only save top 10
			Tuple[] topUsers = new Tuple[10];
			
			for (int i = 0; i < 10; i++) {
				Tuple t = list.get(list.size() - 1 - i);
				System.out.println("User #" + t.userID + " | Reviews #" + t.reviewNum);
			}
		} catch (Exception e) {
			System.out.println("Requires Map-Reduce Ratings output");
		}
	}

	private static class Tuple {
		int userID;
		int reviewNum;

		public Tuple(int id, int num) {
			userID = id;
			reviewNum = num;
		}
	}
}
