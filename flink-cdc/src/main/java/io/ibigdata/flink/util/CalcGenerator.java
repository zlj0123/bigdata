package io.ibigdata.flink.util;

import java.util.Random;

public class CalcGenerator {
    public static void main(String[] args) {
        String[] fuhao = {"+","-"};

        int a,b;
        String c;
        Random random = new Random();

        for (int i = 1; i<=100; i++) {

            c = fuhao[random.nextInt(2)];
            if ("+".equals(c)){
                a = random.nextInt(6);
                b = random.nextInt(6);

                System.out.print(a + " " + c + " " +b + " ="+ "           ");
            }

            if ("-".equals(c)){
                while (true){
                    a = random.nextInt(6);
                    b = random.nextInt(6);
                    if (a >= b){
                        break;
                    }
                }


                System.out.print(a + " " + c + " " +b + " ="+ "           ");
            }

            if (i % 4 == 0) {
                System.out.println();
                System.out.println();
            }
        }
    }
}
