package estado 

import "time"

// Usuario representa os dados cadastrais de um usuário no sistema.
type Usuario struct {
	Login string 
	Senha string
	Nome string
	Papel string
}

// Sessao representa o login ativo de usuário com o seu token
type Sessao struct {
	Token 	string
	Usuario *Usuario
	CriadoEm time.Time
}

