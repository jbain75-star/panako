package be.panako.strategy.panako;
import java.util.*;
/** Synthetic sparse-consensus regressions; no production audio or fingerprints. */
public final class StableAlignmentCheck {
    private static void require(boolean ok,String why){if(!ok)throw new AssertionError(why);}
    private static RobustAlignment.Fit fit(List<int[]> rows){
        int[]q=new int[rows.size()],r=q.clone(),f=q.clone();
        for(int i=0;i<q.length;i++){q[i]=rows.get(i)[0];r[i]=rows.get(i)[1];f[i]=rows.get(i)[2];}
        int[] original=q.clone();
        var fits=RobustAlignment.verifiedFits(q,r,f,40,2,.8,1.2,.8,1.2,85);
        require(Arrays.equals(original,q),"Caller input changed");
        return fits.isEmpty()?null:fits.get(0);
    }
    public static void main(String[]args){
        int checked=0;long started=System.nanoTime();
        for(int seed=0;seed<12;seed++) for(double tempo:new double[]{.94,1.03,1.06}){
            Random random=new Random(seed);List<int[]>rows=new ArrayList<>();
            for(int i=0;i<1600;i++){int q=random.nextInt(3600);rows.add(new int[]{q,random.nextInt(60000),random.nextInt(51)-25});}
            int pitch=seed%9-4;double slope=1-1/tempo;
            for(int i=0;i<64;i++){int q=60+i*54;rows.add(new int[]{q,q+22000+(int)Math.round(slope*q)+(i%3-1),pitch});}
            var first=fit(rows);require(first!=null && first.count>=60,"Lost sparse consensus seed="+seed+" tempo="+tempo);
            require(Math.abs(1/(1-first.slope)-tempo)<.002,"Wrong tempo");
            for(int shuffle=0;shuffle<4;shuffle++){
                Collections.shuffle(rows,random);var next=fit(rows);
                require(next!=null && next.count==first.count && next.slope==first.slope && next.offset==first.offset && next.frequencyDelta==first.frequencyDelta,"Input order changed answer");checked++;
            }
        }
        for(int seed=0;seed<20;seed++){
            Random random=new Random(seed);List<int[]>rows=new ArrayList<>();
            for(int i=0;i<1700;i++){int q=random.nextInt(3600);rows.add(new int[]{q,random.nextInt(60000),random.nextInt(51)-25});}
            require(fit(rows)==null,"Noise accepted");checked++;
        }
        List<int[]>copies=new ArrayList<>();for(int i=0;i<100;i++)copies.add(new int[]{100+i%3,22100+i%3,0});
        require(fit(copies)==null,"Repeated times accepted");checked++;
        for(double tempo:new double[]{.65,1.6}){
            List<int[]>rows=new ArrayList<>();for(int i=0;i<64;i++){int q=60+i*54;rows.add(new int[]{q,q+22000+(int)Math.round((1-1/tempo)*q),0});}
            require(fit(rows)==null,"Out-of-range tempo accepted");checked++;
        }
        System.out.printf("Stable alignment: %d checks passed in %.2fs%n",checked,(System.nanoTime()-started)/1e9);
    }
}
