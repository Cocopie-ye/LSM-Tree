package com.andrea.lsm.core;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
  public static void main(String[] args) {
    DB db = new DB();
    db.put("apple", "red_fruit");
    db.put("banana", "yellow_fruit");
    db.put("cherry", "red_small_fruit");
    db.put("date", "sweet_fruit");
    db.put("elderberry", "purple_fruit");
    db.put("fig", "large_value_".repeat(50));

    System.out.println(db.get("apple"));
    System.out.println(db.get("fig"));
  }
}
