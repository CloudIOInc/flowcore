
package io.cloudio.util;

public class HTML {
  private static final char CLOSE_TAG = '>';
  private static final String endTD = "</td>";
  private static final String endTH = "</th>";
  private static final String eTR = "</tr>";
  private static final char QUOTE = '"';
  private static final String startSTYLE = " style=\"";
  private static final String startTD = "<td";
  private static final String startTH = "<th";
  private static final String TABLE_STYLE = "width: 100%;margin-bottom: 1rem;background-color: transparent;border-collapse: collapse;border: 1px solid #dee2e6;";
  private static final String TABLE_STYLE2 = "<table style=\"";
  private static final String TBODY = "\"><tbody>";
  private static final String TBODY_TABLE = "</tbody></table>";
  private static final String TD_STYLE = "padding: 0.75rem;vertical-align: top;border-top: 1px solid #dee2e6;";
  private static final String TEXT_CENTER_STYLE = "text-align: center;";
  private static final String TR = "<tr>";
  private static final String TR_STYLE = "<tr style=\"background-color: rgba(0, 0, 0, 0.05);\">";
  private StringBuilder body;

  private int row = 1;

  public HTML(StringBuilder body) {
    this.body = body;
  }

  public void alert(String alert) {
    body.append(
        "<div style=\"position: relative;padding: .75rem 1.25rem;margin-bottom: 1rem;border: 1px solid transparent;border-radius: .25rem;color: #856404;background-color: #fff3cd;border-color: #ffeeba;\" role=\"alert\">")
        .append(alert).append("</div></div>");
  }

  public HTML endRow() {
    body.append(eTR);
    return this;
  }

  public HTML endTable() {
    body.append(TBODY_TABLE);
    return this;
  }

  public HTML startRow() {
    if (row % 2 == 0) {
      body.append(TR);
    } else {
      body.append(TR_STYLE);
    }
    row++;
    return this;
  }

  public HTML startTable(String[] titles) {
    row = 1;
    body.append(TABLE_STYLE2).append(TABLE_STYLE).append(TBODY);
    this.startRow();
    for (String title : titles) {
      this.th(title);
    }
    this.endRow();
    return this;
  }

  public HTML td(String val) {
    body.append(startTD).append(startSTYLE).append(TD_STYLE).append(QUOTE).append(CLOSE_TAG).append(val).append(endTD);
    return this;
  }

  public HTML tdCenter(Object val) {
    body.append(startTD).append(startSTYLE).append(TD_STYLE).append(TEXT_CENTER_STYLE).append(QUOTE).append(CLOSE_TAG)
        .append(val)
        .append(endTD);
    return this;
  }

  public HTML th(String title) {
    body.append(startTH).append(startSTYLE).append(TD_STYLE).append(QUOTE).append(CLOSE_TAG).append(title)
        .append(endTH);
    return this;
  }
}
