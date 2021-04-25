/**
 * Pr&aacute;ctricas de Sistemas Inform&aacute;ticos II
 * VisaCancelacionJMSBean.java
 */

package ssii2.visa;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.ejb.EJBException;
import javax.ejb.MessageDriven;
import javax.ejb.MessageDrivenContext;
import javax.ejb.ActivationConfigProperty;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.TextMessage;
import javax.jms.JMSException;
import javax.annotation.Resource;
import java.util.logging.Logger;

/**
 * @author jaime
 */
@MessageDriven(mappedName = "jms/VisaPagosQueue")
public class VisaCancelacionJMSBean extends DBTester implements MessageListener {
  static final Logger logger = Logger.getLogger("VisaCancelacionJMSBean");
  @Resource
  private MessageDrivenContext mdc;

  private static final String UPDATE_CANCELA_QRY =
        "update pago " +
        "set codRespuesta = 999 " +
        "where idAutorizacion = ?";

  private static final String UPDATE_RECTIFY_QRY =
        "update tarjeta as t " +
        "set saldo =  saldo + pago.importe " +
        "from pago " +
        "where pago.idAutorizacion = ? " +
        "and pago.numeroTarjeta = t.numeroTarjeta";


  public VisaCancelacionJMSBean() {
  }

  // TODO : Método onMessage de ejemplo
  // Modificarlo para ejecutar el UPDATE definido más arriba,
  // asignando el idAutorizacion a lo recibido por el mensaje
  // Para ello conecte a la BD, prepareStatement() y ejecute correctamente
  // la actualización
  public void onMessage(Message inMessage) {
      TextMessage msg = null;
      PreparedStatement pstmt = null;
      ResultSet rs = null;
      Integer idAutorizacion;
      Connection con = null;

      try {
          if (inMessage instanceof TextMessage) {
              msg = (TextMessage) inMessage;
              logger.info("MESSAGE BEAN: Message received: " + msg.getText());
              idAutorizacion = Integer.parseInt(msg.getText());
              // Obtener conexion
              con = getConnection();

              String updateSaldo = UPDATE_CANCELA_QRY;
              String rectifySaldo = UPDATE_RECTIFY_QRY;

              pstmt = con.prepareStatement(updateSaldo);
              pstmt.setInt(1, idAutorizacion);
              if (pstmt.execute() || pstmt.getUpdateCount() != 1){
                throw new JMSException("Error en update de codRespuesta");
              }
              pstmt = con.prepareStatement(rectifySaldo);
              pstmt.setInt(1, idAutorizacion);
              if (pstmt.execute() || pstmt.getUpdateCount() != 1){
                throw new JMSException("Error rectificando el saldo");
              }

          } else {
              logger.warning(
                      "Message of wrong type: "
                      + inMessage.getClass().getName());
          }
      } catch (JMSException e) {
          e.printStackTrace();
          mdc.setRollbackOnly();
      } catch (Throwable te) {
          te.printStackTrace();
      }

  }


}
