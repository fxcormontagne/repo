package lo.reporting

import java.sql.ResultSet


class MainController {

    LoQueryService loQueryService
    
    def index() { }
    
    def testLogistics() {
       loQueryService.logisticsDatabaseDirectory = 'D:\\Data\\Logistics\\MP\\MP_062016\\data'
       ResultSet rs = loQueryService.queryMarginData(Date.parse('yyyy-MM-dd','2015-10-01'),Date.parse('yyyy-MM-dd','2015-10-31'),-20.0)
       def data=[]
       while ( rs.next()) {
           data << [ 
                'Vendeur_associe_au_doc':rs.getObject("Vendeur_associe_au_doc"),
                'date':rs.getObject("date"),
                'piece':rs.getObject("piece"),
                'client':rs.getObject("client"),
                'code_article':rs.getObject("code_article")
           ]
       }
       loQueryService.closeConnection()
       render ([qry: loQueryService.lastQuery, result:data])
    }
    
    def test() {
        loQueryService.logisticsDatabaseDirectory = 'D:\\Data\\Logistics\\MP\\MP_062016\\data'
        ResultSet rs = loQueryService.queryTest()
        def data=[]
        while ( rs.next()) {
            data << [ 
                'username':rs.getObject("username")
            ]
        }
       render ([result:data])
    }
}
