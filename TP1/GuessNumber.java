import java.util.Scanner;
import static java.lang.Integer.*;
import static java.lang.Math.*;

public class GuessNumber {

    public static void main(String[] args){

        int trueNum = (int)(random() * 1000000);
        //System.out.println("True number is:" + trueNum);
        int n = -1;
        Scanner reader = new Scanner(System.in);

        do{
            System.out.println("Please enter an integer (between 0 and 999999) or enter \"q\" to quit: ");
            if(reader.hasNextInt()){
                n = reader.nextInt();
                if(n > 999999) {
                    System.out.println("Input out of range.");
                }else if(n > trueNum){
                    System.out.println("Your number is too big.");
                }else if(n < trueNum){
                    System.out.println("Your number is too small.");
                }else{
                    System.out.println("Succeed message!");
                    break;
                }
            }else {
                String blah = reader.next();
                if(blah.equals("q")){
                    System.out.println("Quitting...");
                    break;
                }else {
                    System.out.println("Wrong format! (Your input is either not an INTEGER or bigger than " + MAX_VALUE + "!)");
                }
            }
        }while(true);
    }
}
