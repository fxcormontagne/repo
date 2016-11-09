package lo.reporting

import grails.transaction.Transactional
//import com.jacob.com.*
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.PreparedStatement


@Transactional
class LoQueryService {
    
    static public String logisticsDatabaseDirectory = null
    static private Connection logisticsConn = null
    static public lastQuery = null
    
    public void setLogisticsDatabaseDirectory(String dir) {
        if (logisticsDatabaseDirectory!=null) {
            if (logisticsDatabaseDirectory != dir)
            throw new IllegalStateException("Class "+this.class.name+" is already initialized and connected to a different logisticsDatabaseDirectory\r\n\tCLose connection first.\r\n\tCurrent directory: [${logisticsDatabaseDirectory}]\r\n\tNew directory : [${dir}]")
        } else {
            logisticsDatabaseDirectory = dir
        }
        if (logisticsDatabaseDirectory==null) {
            if (logisticsConn != null) logisticsConn.close()
        }
        String connstr = "jdbc:dbf:/${logisticsDatabaseDirectory}"
        Class.forName('com.hxtt.sql.dbf.DBFDriver')
        logisticsConn = DriverManager.getConnection(connstr)
    }
    
    public void closeConnection() {
        logisticsDatabaseDirectory = null
    }
    
    private Connection getLogisticsConn() {
        if (logisticsConn == null)
        throw new IllegalStateException("Class "+this.class.name+" is not initialized. logisticsDatabaseDirectory is not set.")
        return logisticsConn
    }
    
    private ResultSet selectQuery(String qry) {
        Connection conn = logisticsConn
        PreparedStatement stmt = null
        ResultSet rs = null
        try {
            stmt = conn.prepareStatement(qry)
            lastQuery = stmt
            rs = stmt.executeQuery()
        }
        finally {
            if (stmt != null) stmt.close()
        }
        return rs
    }

    /**
     * returns a JDBC Query Result
     **/
    ResultSet queryMarginData(Date from, Date to, Float filtreMagre) {
        Connection conn = logisticsConn
        PreparedStatement stmt = null
        ResultSet rs = null
        try {
            stmt = conn.prepareStatement(qrySalesMarginByVendor)
            stmt.setFloat(1,(Float) (filtreMagre / 100))
            stmt.setDate(2,new java.sql.Date(from.getTime()))
            stmt.setDate(3,new java.sql.Date(to.getTime()))
            lastQuery = stmt
            rs = stmt.executeQuery()
        }
        finally {
            //if (stmt != null) stmt.close()
        }
        return rs
    }
    
    ResultSet queryTest() {
        selectQuery(testQuery)
    }
    
    static private final String qrySalesMarginByVendor = " \
    select padr(regexp_substr(iif(nvl(i2.vala2,'')<>'',i2.vala2,iif(nvl(i1.vala2,'')<>'',i1.vala2,iif(nvl(dh.s_creuid,'')<>'',dh.s_creuid,dh.s_moduid))),'^[^ ]+'),8) as Vendeur_retenu,\
       padr(regexp_substr(i1.vala2,'^[^ ]+'),8) as Vendeur_associe_au_doc, \
       padr(regexp_substr(i2.vala2,'^[^ ]+'),8) as Vendeur_associe_au_client, \
       padr(regexp_substr(dh.s_creuid,'^[^ ]+'),8) as Vendeur_ayant_cree_le_doc, \
       padr(regexp_substr(dh.s_moduid,'^[^ ]+'),8) as Vendeur_ayant_modifie_le_doc, \
  	   dh.date, \
  	   year(dh.date) as annee, \
  	   month(dh.date) as mois, \
  	   dh.jnl as journal, \
  	   dh.number as numero, \
  	   dh.jnl+str(dh.number,8) as piece, \
  	   iif(dh.number2>0,dh.jnl2+str(dh.number2,8),'') as doc_lie, \
	   dh.thirdgroup as ref_client,dh.thirdname as client, a.family as code_famille, i3.vala2 as famille, dd.artid as code_article, dd.artname as article, \
	   iif(empty(dd.c_altppc), iif(dd.costamount>0,'COUT REVIENT LOGISTICS','PRIX REVIENT ART x QTE'), 'PRIX SPECIAL x QTE') as type_cout_revient, \
       iif(dh.type='CC',-1,1)*iif(empty(dd.c_altppc), iif(dd.costamount>0,dd.costamount,round(a.buyprice*(1-a.buydisc/100)*(1+iif(a.expensetyp=2,a.expenses/100,0))+iif(a.expensetyp=1,a.expenses,0),2)*dd.qty), round(dd.c_altppc*dd.qty,2)) as cout_de_revient, \
       iif(dh.type='CC',-1,1)*dd.amount as ca,  \
       iif(dh.type='CC',-1,1)*(dd.amount - iif(empty(dd.c_altppc), iif(dd.costamount>0,dd.costamount,round(a.buyprice*(1-a.buydisc/100)*(1+iif(a.expensetyp=2,a.expenses/100,0))+iif(a.expensetyp=1,a.expenses,0),2)*dd.qty), round(dd.c_altppc*dd.qty,2))) as marge_brute, \
       round((dd.amount - iif(empty(dd.c_altppc), iif(dd.costamount>0,dd.costamount,round(a.buyprice*(1-a.buydisc/100)*(1+iif(a.expensetyp=2,a.expenses/100,0))+iif(a.expensetyp=1,a.expenses,0),2)*dd.qty), round(dd.c_altppc*dd.qty,2))) / dd.amount,3) as marge_pct, \
       iif(round((dd.amount - iif(empty(dd.c_altppc), iif(dd.costamount>0,dd.costamount,round(a.buyprice*(1-a.buydisc/100)*(1+iif(a.expensetyp=2,a.expenses/100,0))+iif(a.expensetyp=1,a.expenses,0),2)*dd.qty), round(dd.c_altppc*dd.qty,2))) / dd.amount,3)>(?),1,0) as filtre_marge_moins20, \
       iif(dd.costamount>0 or dd.c_altppc>0 or a.buyprice>0,1,0) as filtre_marge_100pc \
    from dochead dh \
        inner join docdet dd on dh.jnl+str(dh.number,8)=dd.jnl+str(dd.number,8) \
        inner join art a on dd.artid=a.artid \
        inner join cust c on dh.thirdgroup=c.groupid \
        left join ( \
              incodes i1 inner join codes t1 on t1.name='AGENT' and t1.tableid=i1.tableid \
        ) on i1.vala1=dh.agent \
        left join ( \
              incodes i2 inner join codes t2 on t2.name='AGENT' and t2.tableid=i2.tableid \
        ) on i2.vala1=c.agent \
        left join ( \
              incodes i3 inner join codes t3 on t3.name='ARTFAMILY' and t3.tableid=i3.tableid \
        ) on i3.vala1=a.family \
    where dh.type in ('CI','CC') \
      and dh.date between ? and ?  \
      and dd.amount<>0 \
      and dd.artid <> 'ZACOMPTE' \
    order by dh.s_credate,dh.jnl,dh.number"
    
    static private final String testQuery = " \
    select regexp_substr(vala2,'^[^ ]+') as username from incodes i inner join codes c on c.name='AGENT' and c.tableid=i.tableid  \
    "
}
