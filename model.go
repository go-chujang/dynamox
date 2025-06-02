package dynamox

type TTL struct {
	ExpiresAt Time `dynamodbav:"expiresAt" json:"expiresAt"`
}

type CreatedAtOnly struct {
	CreatedAt TimeDefaultNow `dynamodbav:"createdAt" json:"createdAt"`
}

type Model struct {
	CreatedAtOnly
	UpdatedAt Time  `dynamodbav:"updatedAt,omitempty" json:"updatedAt"`
	DeletedAt int64 `dynamodbav:"deletedAt" json:"deletedAt"`
}

func (m Model) GetUpdatedAt() int64 { return m.UpdatedAt.Int64() }
func (m Model) GetDeletedAt() int64 { return m.DeletedAt }

func (m *Model) SetUpdatedAt() *Model {
	m.UpdatedAt = TimeNow()
	return m
}

func (cud *Model) SetDeletedAt() *Model {
	cud.DeletedAt = TimeNow().Int64()
	return cud
}

/////////////////////////////////////////////////////////////////////////////

func HasModel(item KeyedItem) bool { return hasEmbeddedStruct(item, (*Model)(nil)) }
func SetUpdatedAt(item KeyedItem)  { callMethod(item, (*Model)(nil), "SetUpdatedAt") }
func SetDeletedAt(item KeyedItem)  { callMethod(item, (*Model)(nil), "SetDeletedAt") }
func GetDeletedAt(item KeyedItem) (int64, error) {
	res, err := callMethod(item, (*Model)(nil), "GetDeletedAt")
	if err != nil {
		return 0, err
	}
	deletedAt, _ := res[0].(int64)
	return deletedAt, nil
}
