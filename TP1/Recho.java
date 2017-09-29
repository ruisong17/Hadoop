public class Recho {

    public static void main(String[] args){
        if(args.length == 0) return;
        String str = "";
        for(int i = args.length-1; i >= 0; i--){
            str = str + args[i] + " ";
        }
        System.out.println(str.trim());
    }
}
