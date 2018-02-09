package core;

public class TableManager {
    public static void main(String[] args){
        TablesCreator db = new TablesCreator("fly_companies");
//        db.deleteAllTables();
//        db.createTables();
        db.fillTables();
//        System.out.println(Arrays.toString(db.showTables()));
    }
}
