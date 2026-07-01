package main

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"log"
	"net/http"
	"net/url"
	"os"

	"github.com/crewjam/saml/samlsp"
)

// roleClaimNames are the attribute names under which IdPs deliver app role
// assignments. Azure AD / Entra ID uses the full claim URI.
var roleClaimNames = []string{
	"http://schemas.microsoft.com/ws/2008/06/identity/claims/role",
	"roles",
	"role",
}

// setupSAML initialises the SAML middleware when SAML_ENABLED=true.
// Returns nil (no-op) when the feature is disabled.
func setupSAML() *samlsp.Middleware {
	if os.Getenv("SAML_ENABLED") != "true" {
		return nil
	}

	certFile := envOr("SAML_SP_CERT_FILE", "./certs/saml/sp.crt")
	keyFile := envOr("SAML_SP_KEY_FILE", "./certs/saml/sp.key")

	keyPair, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		log.Fatalf("saml: load cert/key: %v", err)
	}
	keyPair.Leaf, err = x509.ParseCertificate(keyPair.Certificate[0])
	if err != nil {
		log.Fatalf("saml: parse cert: %v", err)
	}

	idpMetadataRaw := os.Getenv("SAML_IDP_METADATA_URL")
	if idpMetadataRaw == "" {
		log.Fatal("saml: SAML_IDP_METADATA_URL must be set when SAML_ENABLED=true")
	}
	idpMetadataURL, err := url.Parse(idpMetadataRaw)
	if err != nil {
		log.Fatalf("saml: parse IDP metadata URL: %v", err)
	}

	rootURLRaw := os.Getenv("SAML_SP_ROOT_URL")
	if rootURLRaw == "" {
		log.Fatal("saml: SAML_SP_ROOT_URL must be set when SAML_ENABLED=true")
	}
	rootURL, err := url.Parse(rootURLRaw)
	if err != nil {
		log.Fatalf("saml: parse SP root URL: %v", err)
	}

	idpMetadata, err := samlsp.FetchMetadata(context.Background(), http.DefaultClient, *idpMetadataURL)
	if err != nil {
		log.Fatalf("saml: fetch IDP metadata from %s: %v", idpMetadataRaw, err)
	}

	middleware, err := samlsp.New(samlsp.Options{
		URL:         *rootURL,
		Key:         keyPair.PrivateKey.(*rsa.PrivateKey),
		Certificate: keyPair.Leaf,
		IDPMetadata: idpMetadata,
	})
	if err != nil {
		log.Fatalf("saml: init middleware: %v", err)
	}

	log.Printf("saml: enabled, SP entity ID: %s/saml/metadata", rootURLRaw)
	return middleware
}

// hasAdminRole reports whether the SAML session carries adminRole. An empty
// adminRole (feature unconfigured) or disabled SAML grants access to everyone.
func hasAdminRole(r *http.Request, samlEnabled bool, adminRole string) bool {
	if !samlEnabled || adminRole == "" {
		return true
	}
	sa, ok := samlsp.SessionFromContext(r.Context()).(samlsp.SessionWithAttributes)
	if !ok {
		return false
	}
	attrs := sa.GetAttributes()
	for _, name := range roleClaimNames {
		for _, v := range attrs[name] {
			if v == adminRole {
				return true
			}
		}
	}
	return false
}
