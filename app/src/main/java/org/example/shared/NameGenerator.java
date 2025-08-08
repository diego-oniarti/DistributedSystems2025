package org.example.shared;

import java.io.File;
import java.util.Scanner;

public class NameGenerator {
    private static final NameGenerator single = new NameGenerator();
    public RngList<String> fruitNames;
    
    public NameGenerator() {
        fruitNames = new RngList<>();

        File f = new File("fruits.txt");
        try {
            Scanner scan = new Scanner(f);
            while (scan.hasNextLine()) {
                String fruit = scan.nextLine();
                fruitNames.add(fruit);
            }
            scan.close();
        } catch (Exception e) {
            System.err.println("Failed to read fruit names: "+e.getMessage());
        }
    }

    public static String getFruit() {
        return single.fruitNames.removeRandom();
    }
}
