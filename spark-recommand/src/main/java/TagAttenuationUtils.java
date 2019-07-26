/**
 * 标签衰减系数计算工具
 */
public class TagAttenuationUtils {

    /**
     * 获取某一天的衰减系数
     *
     * @param T                 标签衰减的一个周期
     * @param intervalDays      标签最后一次访问时间距离当前的天数，这里以天为单位
     * @param initialValueTimes 通过一个周期的衰减，最终值要达到的初始值的倍数，取值范围：0-1之间的小数，比如：0.1,0.5,0.9等
     * @return
     */
    public static double getDaysConstant(int T, int intervalDays, double initialValueTimes) {
        //温度衰减公式为：F(t)=初始温度*exp(-冷却系数×间隔的时间)
        // 其中α为衰减常数，通过回归可计算得出。
        // 例如：指定45分钟后物体温度为初始温度的0.5，即 0.5=1×exp(-a×45)，求得α=0.1556。
        // 这个公式应用在排序上即是
        //本次评分 = 上次评分* exp（-衰减常数 * 间隔时间）+当日评分

        int newIntervalDays = intervalDays % T;// 35%30=5，
        if (T > 0 && intervalDays > 0 && (initialValueTimes > 0 && initialValueTimes < 1)) {

            double constantValue = 0.0;
            constantValue = Math.log(initialValueTimes) / (T * -1);
            return Math.exp(-constantValue * newIntervalDays);
        } else {
            return 0.9d;//给出默认的衰减系数
        }
    }

    /**
     * 获取以30天为一个周期的某一天的衰减系数
     * @param intervalDays 标签最后一次访问时间距离当前的天数，这里以天为单位
     * @param initialValueTimes 通过一个周期的衰减，最终值要达到的初始值的倍数，取值范围：0-1之间的小数，比如：0.1,0.5,0.9等
     * @return
     */
    public static double get30TDaysConstant(int intervalDays, double initialValueTimes){
        return getDaysConstant(30,intervalDays,initialValueTimes);
    }

    /**
     * 获取以60天为一个周期的某一天的衰减系数
     * @param intervalDays 标签最后一次访问时间距离当前的天数，这里以天为单位
     * @param initialValueTimes 通过一个周期的衰减，最终值要达到的初始值的倍数，取值范围：0-1之间的小数，比如：0.1,0.5,0.9等
     * @return
     */
    public static double get60TDaysConstant(int intervalDays, double initialValueTimes){
        return getDaysConstant(60,intervalDays,initialValueTimes);
    }
}
