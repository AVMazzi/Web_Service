using BLL;
using DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;

namespace AspnWebServiceDB
{
    public partial class Default : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            if (!this.IsPostBack)
            {
                this.VinculaDados();
            }
        }
        private void VinculaDados()
        {
            ServiceDB service = new ServiceDB();
            gdvProdutos.DataSource = service.GetUser();
            gdvProdutos.DataBind();
        }
        private void SalvaDados()
        {
            TB_USER objUser = new TB_USER();
            objUser.NM_USER = txtUser.Text;
            objUser.DS_EMAIL = txtEmail.Text;
            ServiceDB service = new ServiceDB();
            service.SaveUser(objUser);

        }
        
        void btnSave_Click(object sender,  EventArgs e)
        {
            
        }

        protected void btnListar_Click(object sender, EventArgs e)
        {
            this.VinculaDados();
        }

        protected void btnSalvar_Click(object sender, EventArgs e)
        {
            this.SalvaDados();
        }
    }
}