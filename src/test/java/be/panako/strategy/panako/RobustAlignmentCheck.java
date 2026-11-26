package be.panako.strategy.panako;

import java.util.*;

/** Standalone checks: synthetic data only, no audio or production fixtures. */
public final class RobustAlignmentCheck {
    private static void require(boolean condition, String message) {
        if (!condition) throw new AssertionError(message);
    }
    private static RobustAlignment.Fit fit(int[][] rows) {
        int n=rows.length; int[] q=new int[n], r=new int[n], f=new int[n];
        Arrays.sort(rows, Comparator.comparingInt(a -> a[0]));
        for(int i=0;i<n;i++){q[i]=rows[i][0];r[i]=rows[i][1];f[i]=rows[i][2];}
        return RobustAlignment.fit(q,r,f,40,2,0.8,1.2,0.8,1.2,85);
    }
    private static int[][] fixture(long seed, double time, int pitch, boolean signal) {
        Random random=new Random(seed);
        int[][] rows=new int[signal?250:2000][3];
        double slope=1-1/time;
        for(int i=0;i<rows.length;i++) {
            if(signal && i<50) {
                int q=50+i*60;
                rows[i]=new int[]{q,q+(int)Math.round(232+slope*q)+(i%3-1),pitch+(i%5==0?1:0)};
            }else{
                int q=random.nextInt(3100);
                rows[i]=new int[]{q,random.nextInt(40000),random.nextInt(100)-50};
            }
        }
        return rows;
    }
    public static void main(String[] args) {
        int checks=0;
        for(double time:new double[]{0.94,0.97,1,1.03,1.06}) {
            for(int pitch:new int[]{-7,0,7}) for(int seed=0;seed<12;seed++) {
                int[][] rows=fixture(seed,time,pitch,true);
                RobustAlignment.Fit answer=fit(rows);
                require(answer!=null && answer.count>=45,"Lost true line: "+time+" / "+pitch+" / "+seed);
                require(Math.abs(1/(1-answer.slope)-time)<0.002,"Wrong tempo");
                RobustAlignment.Fit again=fit(rows);
                require(again.count==answer.count && again.slope==answer.slope && again.offset==answer.offset,
                        "Non-deterministic fit");
                checks++;
            }
        }
        for(int seed=0;seed<100;seed++) {
            require(fit(fixture(seed,1,0,false))==null,"Random collisions became a match");checks++;
        }
        for(double time:new double[]{0.65,1.6}) {
            require(fit(fixture(91,time,0,true))==null,"Accepted out-of-range tempo");checks++;
        }
        require(fit(fixture(91,1,50,true))==null,"Accepted out-of-range pitch");checks++;
        int[][] repeated=new int[100][3];
        for(int i=0;i<100;i++) repeated[i]=new int[]{100+i%3,332+i%3,0};
        require(fit(repeated)==null,"Repeated copies of three times became evidence");checks++;
        // Exact-bin modes favour a repeated beat (21 votes) over each noisy
        // bin of the real offset (19/18/9). Neighbour consensus must win.
        int[][] jitter=new int[100][3];
        for(int i=0;i<46;i++){int q=100+i*60;int d=i<19?232:i<37?231:230;jitter[i]=new int[]{q,q+d,0};}
        for(int i=46;i<67;i++){int q=100+(i-46)*60;jitter[i]=new int[]{q,q+12324,0};}
        for(int i=67;i<100;i++){int q=100+(i-67)*60;jitter[i]=new int[]{q,q+9000+i*45,0};}
        RobustAlignment.Fit recovered=fit(jitter);
        require(recovered!=null && recovered.count>=40,"Failed split-bin timing regression");checks++;
        System.out.println("Robust alignment: "+checks+" synthetic checks passed");
    }
}
