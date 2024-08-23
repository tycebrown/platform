import ModalView, { ModalViewTitle } from "@/app/components/ModalView";
import Button from "@/app/components/Button";
import FormLabel from "@/app/components/FormLabel";
import TextInput from "@/app/components/TextInput";
import AcceptInviteForm from './AcceptInviteForm'
import FieldError from '@/app/components/FieldError';
import { getTranslations } from 'next-intl/server';
import { query } from '@/app/db';
import { notFound } from 'next/navigation';

interface Props {
    params: { locale: string },
    searchParams: { token?: string }
}

export default async function LoginPage({ params, searchParams }: Props) {
    const t = await getTranslations('AcceptInvitePage');

    if (!searchParams.token) {
        notFound()
    }

    const inviteQuery = await query<{ email: string }>(`SELECT email FROM "UserInvitation" AS i JOIN "User" AS u ON u.id = i."userId" WHERE i.token = $1`, [searchParams.token])
    const invite = inviteQuery.rows[0]
    if (!invite) {
        notFound()
    }

    return <ModalView className="max-w-[480px] w-full"
        header={
        <Button href={`/${params.locale}/login`} variant="tertiary">
          Log In
        </Button>
      }
    >
        <ModalViewTitle>{t('title')}</ModalViewTitle>
        <AcceptInviteForm>
            <input type="hidden" name="token" value={searchParams.token} />
            <div className="mb-4">
              <FormLabel htmlFor="email">
                {t('form.email')}
              </FormLabel>
              <TextInput
                id="email"
                className="w-full bg-gray-200"
                readOnly
                defaultValue={inviteQuery.rows[0].email}
              />
            </div>
            <div className="flex gap-4 mb-4">
              <div className="flex-1 w-full">
                <FormLabel htmlFor="first-name">
                  {t('form.first_name')}
                </FormLabel>
                <TextInput
                  id="first-name"
                  name="first_name"
                  className="w-full"
                  autoComplete="given-name"
                  aria-describedby="first-name-error"
                />
                <FieldError id="first-name-error" name="first_name" />
              </div>
              <div className="flex-1 w-full">
                <FormLabel htmlFor="last-name">
                  {t('form.last_name')}
                </FormLabel>
                <TextInput
                  id="last-name"
                  className="w-full"
                  name="last_name"
                  autoComplete="family-name"
                  aria-describedby="last-name-error"
                />
                <FieldError id="last-name-error" name="last_name" />
              </div>
            </div>
            <div className="mb-4">
              <FormLabel htmlFor="password">
                {t('form.password')}
              </FormLabel>
              <TextInput
                type="password"
                id="password"
                name="password"
                className="w-full"
                autoComplete="new-password"
                aria-describedby="password-error"
              />
              <FieldError id="password-error" name="password" />
            </div>
            <div className="mb-6">
              <FormLabel htmlFor="confirm-password">
                {t('form.confirm_password').toUpperCase()}
              </FormLabel>
              <TextInput
                type="password"
                id="confirm-password"
                name="confirm_password"
                className="w-full"
                autoComplete="new-password"
                aria-describedby="confirm-password-error"
              />
              <FieldError id="confirm-password-error" name="confirm_password" />
            </div>
            <Button type="submit" className="w-full mb-2">{t('form.submit')}</Button>
        </AcceptInviteForm>
    </ModalView>
}
