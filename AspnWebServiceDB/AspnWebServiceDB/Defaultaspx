<%@ Page Language="C#" AutoEventWireup="true" CodeBehind="Default.aspx.cs" Inherits="AspnWebServiceDB.Default" %>

<!DOCTYPE html>
<html xmlns="http://www.w3.org/1999/xhtml">
<head runat="server">
    <title></title>
    <style type="text/css">
        .auto-style1 {
            color: #0033CC;
        }
    </style>
</head>
<body>
    <form id="form1" runat="server">
        <h1 class="auto-style1">Relação de Usuários</h1>
        <hr />
        <div>
            <asp:Table ID="Table1" runat="server">
                <asp:TableRow
                    ID="TableRow1"
                    runat="server">
                    <asp:TableCell>
                        <asp:Label ID="Label1" runat="server" Text="Usuário: "></asp:Label>
                    </asp:TableCell>
                    <asp:TableCell>
                        <asp:TextBox ID="txtUser" runat="server"></asp:TextBox>
                    </asp:TableCell>
                </asp:TableRow>
                <asp:TableRow
                    ID="TableRow2"
                    runat="server">
                    <asp:TableCell>
                        <asp:Label ID="Label2" runat="server" Text="E-mail: "></asp:Label>
                    </asp:TableCell>
                    <asp:TableCell>
                        <asp:TextBox ID="txtEmail" runat="server"></asp:TextBox>
                    </asp:TableCell>
                </asp:TableRow>
                  <asp:TableRow 
                ID="TableRow3" 
                runat="server">
                    <asp:TableCell>
                        <%--<asp:Button ID="btnSave" runat="server" Text="Save" OnClick="btnSave_Click" />--%>  
                        <asp:Button ID="btnSalvar" runat="server" Text="Save" OnClick="btnSalvar_Click"/>
                    </asp:TableCell>
                    <asp:TableCell>
                        <asp:Button ID="btnListar" runat="server" Text="List" OnClick="btnListar_Click"/>
                    </asp:TableCell>                
            </asp:TableRow>
            </asp:Table>
        </div>

        <div>
            <asp:GridView ID="gdvProdutos" runat="server" AutoGenerateColumns="false">
                <Columns>
                    <asp:TemplateField HeaderText="Cod." ItemStyle-Width="50">
                        <ItemTemplate>
                            <asp:Label ID="lblId" runat="server" Text='<%# Eval("CD_USER") %>'></asp:Label>
                        </ItemTemplate>
                    </asp:TemplateField>
                    <asp:TemplateField HeaderText="Usuário" ItemStyle-Width="150">
                        <ItemTemplate>
                            <asp:Label ID="lblNome" runat="server" Text='<%# Eval("NM_USER") %>'></asp:Label>
                        </ItemTemplate>
                    </asp:TemplateField>
                    <asp:TemplateField HeaderText="E-Mail" ItemStyle-Width="150">
                        <ItemTemplate>
                            <asp:Label ID="lblPreco" runat="server" Text='<%# Eval("DS_EMAIL") %>'></asp:Label>
                        </ItemTemplate>
                    </asp:TemplateField>
                </Columns>
            </asp:GridView>
        </div>
    </form>
</body>
</html>
