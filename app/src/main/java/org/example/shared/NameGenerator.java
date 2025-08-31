package org.example.shared;

import java.io.File;
import java.util.Random;
import java.util.Scanner;

public class NameGenerator {
    private static final NameGenerator single = new NameGenerator();
    private RngList<String> fruitNames;
    private RngList<String> peopleNames;
    private static Random rng = new Random();
    
    private NameGenerator() {
        fruitNames = new RngList<>();
        peopleNames = new RngList<>();

        loadFruit();
        loadPeople();
    }

    private void loadFruit() {
        File f = new File("app/fruits.txt");
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

    private void loadPeople() {
        File f = new File("app/names.txt");
        try {
            Scanner scan = new Scanner(f);
            while (scan.hasNextLine()) {
                String name = scan.nextLine();
                peopleNames.add(name);
            }
            scan.close();
        } catch (Exception e) {
            System.err.println("Failed to read people names: "+e.getMessage());
        }
    }

    public static String getFruit() {
        if (single.fruitNames.isEmpty()) return generateRandomString(8);
        return single.fruitNames.removeRandom();
    }

    public static String getName() {
        if (single.peopleNames.isEmpty()) return generateRandomString(8);
        return single.peopleNames.removeRandom();
    }

    public static String generateRandomString(int length) {
        String characterSet = "abcdefghijklmnopqrstuvwxyz";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            int index = rng.nextInt(characterSet.length());
            sb.append(characterSet.charAt(index));
        }
        return sb.toString();
    }
}
